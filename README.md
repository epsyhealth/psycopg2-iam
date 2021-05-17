# psycopg2-iam

Custom Connection Factory class with build-in IAM authentication and SSL bundle downloader support.

## Usage

### Using connect function 

```python
from psycopg2_iam import connect
connect(dsn="...")
```

### Pass connection factory class to psycopg2.connect()

```python
import psycopg2 
from psycopg2_iam import IAMConnection

psycopg2.connect(dsn="...", connection_factory=IAMConnection)
```


### Create DSN from AWS generated RDS secret


```python
import boto3
import json
import psycopg2 
from psycopg2_iam import IAMConnection, dsn_from_rds_secret

secrets = boto3.client("secretsmanager")
db_secret = json.loads(secrets.get_secret_value("rds-secret"))

psycopg2.connect(dsn=dsn_from_rds_secret(db_secret), connection_factory=IAMConnection)
```
