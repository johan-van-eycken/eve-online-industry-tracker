from sqlalchemy import Column, Integer, String, Text, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base

# Base is the declarative base for SQLAlchamy models
Base = declarative_base()

# Define the ESI Cache table as an ORM model
class EsiCache(Base):
    __tablename__ = "esi_cache"

    endpoint = Column(Text, primary_key=True)
    etag = Column(Text)
    data = Column(Text) # JSON data
    last_updated = Column(Float)

# Define the Characters table as an ORM model
class OAuthCharacter(Base):
    __tablename__ = "characters"

    id = Column(Integer, primary_key=True, autoincrement=True)
    character_name = Column(String, unique=True, nullable=False)
    character_id = Column(Integer)
    refresh_token = Column(String, nullable=False)
    scopes = Column(Text)  # JSON-formatted string of scopes
    is_main = Column(Boolean, default=False)
