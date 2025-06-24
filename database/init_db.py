from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

DATABASE_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "user": "jatinkumarparmar",
    "password": os.getenv("DB_PASSWORD"),
    "dbname": "airflow_db",
}

DATABASE_URL = (
    f"postgresql://{DATABASE_CONFIG['user']}:{DATABASE_CONFIG['password']}"
    f"@{DATABASE_CONFIG['host']}:{DATABASE_CONFIG['port']}/{DATABASE_CONFIG['dbname']}"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

def init_db():
    from models import Base
    Base.metadata.create_all(engine)