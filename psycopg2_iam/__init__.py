import logging
import tempfile
from os.path import join, isfile

import boto3
import psycopg2
import psycopg2.extensions
from psycopg2._psycopg import parse_dsn

logger = logging.getLogger(__name__)


class IAMConnection(psycopg2.extensions.connection):
    def __init__(self, dsn, *more):
        parsed = parse_dsn(dsn)
        if "password" in parsed:
            logger.warning("Provided DSN contains password, IAM authentication will be disabled.")
        else:
            if not all([parsed.get("host"), parsed.get("port"), parsed.get("user")]):
                raise psycopg2.ProgrammingError("IAM connection require: host, port and username to be provided")

            client = boto3.client("rds")
            parsed["password"] = client.generate_db_auth_token(
                DBHostname=parsed.get("host"),
                Port=parsed.get("port"),
                DBUsername=parsed.get("user"),
            )
            parsed["sslmode"] = "verify-full"
            if "sslrootcert" not in parsed:
                parsed["sslrootcert"] = self._get_bundle_cert()

            dsn = psycopg2.extensions.make_dsn(**parsed)

        super().__init__(dsn, *more)

    def _get_bundle_cert(self):
        import urllib.request
        import hashlib
        bundle_path = join(tempfile.gettempdir(), "rds-bundle.crt")

        if isfile(bundle_path):
            url = "https://s3.amazonaws.com/rds-downloads/rds-combined-ca-bundle.pem"
            with urllib.request.urlopen(url) as source, open(bundle_path, "wb+") as dest:
                bundle = source.read()
                if hashlib.md5(bundle).hexdigest() != "0507597169bd025f95803b0d1713f943":
                    raise RuntimeError("Failed to download bundle certificate. Checksum failed.")
                dest.write(bundle)

        return bundle_path


def connect(dsn=None, cursor_factory=None, **kwargs) -> psycopg2.extensions.connection:
    return psycopg2.connect(dsn, connection_factory=IAMConnection, cursor_factory=cursor_factory, **kwargs)


def dsn_from_rds_secret(secret: dict) -> str:
    params = dict(
        host=secret.get("host"),
        port=secret.get("port"),
        dbname=secret.get("dbname"),
        user=secret.get("username")
    )

    if "password" in secret:
        params["password"] = secret.get("password")

    return psycopg2.extensions.make_dsn(**params)
