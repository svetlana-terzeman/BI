from sqlalchemy                 import Column, Integer, String, JSON, DateTime, create_engine,TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql             import func
from settings.constants         import USERNAME, PASSWD, HOST, PORT, DB, tablename

DATABASE_URL = f"postgresql://{USERNAME}:{PASSWD}@{HOST}:{PORT}/{DB}"
engine       = create_engine(DATABASE_URL)
Base         = declarative_base()


class BITaskRegister(Base):
    __tablename__ = tablename

    id              = Column(Integer, primary_key=True, index=True)
    dag_name        = Column(String(100), nullable=False)
    task_metadata   = Column(JSON, default={}, nullable=False)
    status          = Column(Integer, default=0, nullable=False)
    filename        = Column(String(150), nullable=False)
    timestamp       = Column(DateTime(timezone=True), server_default=func.now())



Base.metadata.create_all(bind=engine)
print("Таблица bi__task_register успешно создана")