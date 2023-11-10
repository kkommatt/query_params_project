from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database


Base = declarative_base()

engine = create_engine(
    "sqlite:///ToDoDB.db",
    connect_args={"check_same_thread": False},
    echo=True,
)
create_database(engine.url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session = SessionLocal()

if not database_exists(engine.url):
    create_database(engine.url)
    Base.metadata.create_all(bind=engine)
