from sqlalchemy import Column, Integer, String, BigInteger, DateTime, func, UniqueConstraint
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class SuscripcionTema(Base):
    __tablename__ = "suscripciones_tema"
    __table_args__ = (UniqueConstraint('user_id', 'tema'),)
    suscripcion_id = Column(Integer, primary_key=True, index=True)
    fecha = Column(DateTime(timezone=True), server_default=func.now())
    user_id = Column(BigInteger, nullable=False, index=True)
    tema = Column(String(50), nullable=False)

class SuscripcionDataset(Base):
    __tablename__ = "suscripciones_dataset"
    __table_args__ = (UniqueConstraint('user_id', 'dataset'),)
    suscripcion_id = Column(Integer, primary_key=True, index=True)
    fecha = Column(DateTime(timezone=True), server_default=func.now())
    user_id = Column(BigInteger, nullable=False, index=True)
    dataset = Column(String(255), nullable=False)

class SuscripcionNodo(Base):
    __tablename__ = "suscripciones_nodo"
    __table_args__ = (UniqueConstraint('user_id', 'nodo'),)
    suscripcion_id = Column(Integer, primary_key=True, index=True)
    fecha = Column(DateTime(timezone=True), server_default=func.now())
    user_id = Column(BigInteger, nullable=False, index=True)
    nodo = Column(String(255), nullable=False)


