# app/test_db.py
from app.db.database import engine, base
# It's crucial to import the models here, so that SQLAlchemy knows about them when we call create_all()
from app.db.models import Athlete, Match, MatchPerformance, SetPerformance

def init_db():
    print("Is connecting to PostgreSQL and creating tables...")
    try:
        # it will compare the database, and if the tables don't exist, it will create them
        base.metadata.create_all(bind=engine)
        print("The four tables have been successfully created in the database!")
    except Exception as e:
        print(f"Failed to create tables:\n{e}")

if __name__ == "__main__":
    init_db()