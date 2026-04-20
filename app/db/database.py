import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/taekwondo_db")

# create the SQLAlchemy engine, which will manage the connection pool and database interactions
engine = create_engine(DATABASE_URL)

# FastAPI uses SQLAlchemy's sessionmaker to create a new database session for each request
sessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# for model.py, we need to create a base class that our models will inherit from
base = declarative_base()

# Dependency function to get a database session for each request
def get_db():
    db = sessionLocal()
    try:
        yield db
    finally:
        db.close()