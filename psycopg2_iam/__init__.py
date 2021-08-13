import json
import logging
import tempfile
from abc import ABC, abstractmethod
from os.path import isfile, join

import boto3
import psycopg2
import psycopg2.extensions
from psycopg2._psycopg import parse_dsn

logger = logging.getLogger(__name__)


class IAMConnection(ABC, psycopg2.extensions.connection):
    def __init__(self, dsn, *more):
        parsed = parse_dsn(dsn)
        if not all([parsed.get("host"), parsed.get("port"), parsed.get("user")]):
            raise psycopg2.ProgrammingError("IAM connection require: host, port and username to be provided")

        self._set_credentials(parsed)

        parsed["sslmode"] = "verify-full"
        if "sslrootcert" not in parsed:
            parsed["sslrootcert"] = self._get_bundle_cert()

        dsn = psycopg2.extensions.make_dsn(**parsed)

        logger.debug(f"Connecting with dsn: {dsn}")

        super().__init__(dsn, *more)

    def _get_bundle_cert(self):
        import hashlib
        import urllib.request

        bundle_path = join(tempfile.gettempdir(), f"{self.__class__.__name__}-bundle.crt")
        logger.debug(f"Fetching bundle certificate")

        if not isfile(bundle_path):
            with urllib.request.urlopen(self._get_ca_bundle_url()) as source, open(bundle_path, "wb+") as dest:
                bundle = source.read()
                if hashlib.md5(bundle).hexdigest() != self._get_ca_bundle_hash():
                    raise RuntimeError("Failed to download bundle certificate. Checksum failed.")
                dest.write(bundle)
                dest.flush()

        logger.debug(f"Certificate stored: {bundle_path}")

        return bundle_path

    @abstractmethod
    def _set_credentials(self, parsed: dict):
        pass

    @abstractmethod
    def _get_ca_bundle_url(self):
        pass

    @abstractmethod
    def _get_ca_bundle_hash(self):
        pass


class RDSIAMConnection(IAMConnection):
    def _set_credentials(self, parsed: dict):
        client = boto3.client("rds")
        parsed["password"] = client.generate_db_auth_token(
            DBHostname=parsed.get("host"),
            Port=parsed.get("port"),
            DBUsername=parsed.get("user"),
        )

        return parsed

    def _get_ca_bundle_url(self):
        return "https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem"

    def _get_ca_bundle_hash(self):
        return "0507597169bd025f95803b0d1713f943"


class RedshiftConnection(IAMConnection):
    def _set_credentials(self, parsed: dict):
        client = boto3.client("redshift")
        credentials = client.get_cluster_credentials(
            DbUser=parsed["user"],
            DbName=parsed["dbname"],
            ClusterIdentifier=parsed["host"].split(".")[0],
            DurationSeconds=15 * 60,
            AutoCreate=True,
        )
        parsed["user"] = credentials.get("DbUser")
        parsed["password"] = credentials.get("DbPassword")

        return parsed

    def _get_ca_bundle_url(self):
        return "https://s3.amazonaws.com/redshift-downloads/redshift-ca-bundle.crt"

    def _get_ca_bundle_hash(self):
        return "a3081eb4a7448982df1c23cd6aec9a87"


def connect(dsn=None, secret=None, cursor_factory=None, **kwargs) -> psycopg2.extensions.connection:
    if secret and not dsn:
        logger.debug(f"Downloading secret {secret}")
        secrets = boto3.client("secretsmanager")
        db_secret = json.loads(secrets.get_secret_value(SecretId=secret).get("SecretString"))
        dsn = dsn_from_rds_secret(db_secret)

    conn_cls = psycopg2.extensions.connection
    if "redshift.amazonaws.com" in dsn:
        conn_cls = RedshiftConnection
    elif "rds.amazonaws.com" in dsn:
        conn_cls = RDSIAMConnection

    return psycopg2.connect(dsn, connection_factory=conn_cls, cursor_factory=cursor_factory, **kwargs)


def dsn_from_rds_secret(secret: dict) -> str:
    params = dict(
        host=secret.get("host"), port=secret.get("port"), dbname=secret.get("dbname"), user=secret.get("username")
    )

    if "password" in secret:
        params["password"] = secret.get("password")

    return psycopg2.extensions.make_dsn(**params)
