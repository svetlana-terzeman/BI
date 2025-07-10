from fastapi import FastAPI, Depends, HTTPException, status, Request
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel, Field, validator
from typing import Dict, Any, Type
import logging
import json
import os
import sys
import requests
from pathlib import Path
from datetime import datetime, timedelta
from settings.constants import USERNAME, PASSWD, HOST, PORT, DB, scheme_forms, status_name, external_IP, external_port
from libs.scheme_db import BITaskRegister, Base  # Импортируем из scheme_db

# Настройка логов

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# Подключение к БД
DATABASE_URL = f"postgresql://{USERNAME}:{PASSWD}@{HOST}:{PORT}/{DB}"
engine       = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Pydantic схемы
class TaskCreate(BaseModel):
    dag_name     : str            = Field("LTV", min_length=1, max_length=100)
    task_metadata: Dict[str, Any] = Field({"CURRDATE" : "2025-06-15", "RESEARCH_PERIOD": 90})

    @validator('dag_name')
    def validate_dag_name(cls, v):
        v = v.strip()
        if not v:
            logger.info("Название DAG не может быть пустым")
            raise ValueError("Название DAG не может быть пустым")
        return v

    @validator('task_metadata')
    def validate_metadata(cls, v: Dict[str, Any], values: Dict[str, Any]) -> Dict[str, Any]:
        """Валидация метаданных задачи"""
        if not isinstance(v, dict):
            logger.error("Metadata must be a dictionary")
            raise ValueError("Метаданные должны быть словарем")

        # Получаем схему из контекста (предполагается, что scheme_forms доступен)
        dag_name = values.get('dag_name')

        if dag_name not in scheme_forms:
            raise ValueError("dag_name указан неверно")

        # Проверка обязательных ключей
        required_keys = scheme_forms[dag_name].keys()
        if not set(required_keys).issubset(v.keys()):
            missing = required_keys - set(v.keys())
            logger.error(f"Отсутствуют обязательные ключи: {missing}")
            raise ValueError(f"Отсутствуют обязательные ключи: {missing}")

        for key in required_keys:
            if scheme_forms[dag_name][key]['required']:
                if not v[key]:
                    logger.error(f"{key} не может быть пустым")
                    raise ValueError(f"{key} не может быть пустым")
            try:
                cls._validate_field(
                    value=v[key],
                    field_type=scheme_forms[dag_name][key]['type']
                )
            except ValueError as e:
                logger.error(f"Поле {key} невозможно преобразовать в {scheme_forms[dag_name][key]['type']}")
                raise ValueError(f"Поле {key} невозможно преобразовать в {scheme_forms[dag_name][key]['type']}")

        return v

    @classmethod
    def _validate_field(cls, value: Any, field_type: Type) -> Any:
        """Валидация отдельного поля по типу"""
        try:
            if field_type == 'datetime':
                datetime.strptime(value, "%Y-%m-%d")
            elif field_type == 'int':
                int(value)
            elif field_type == 'float':
                float(value)
            elif field_type == 'str':
                str(value)
            elif field_type == 'bool':
                if isinstance(value, str):
                    value.lower() in ('true', '1', 'yes')
                else:
                    bool(value)

        except (ValueError, TypeError) as e:
            raise ValueError(f"Невозможно преобразовать в {field_type}")


app = FastAPI(
    title="BI Task Registry API",
    description="API для регистрации BI задач",
    version="1.0.0"
)

app.mount("/static", StaticFiles(directory="result"), name="static")
# @app.get("/download/{filename}")
# def download_file(filename: str):
#     file_path = f"result/{filename}"
#     return FileResponse(
#         path=file_path,
#         filename=filename,
#         media_type='application/octet-stream'
#     )

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def generate_filename(dag_name: str) -> str:
    """Генерирует имя файла по шаблону dag_name__vYYYYMMDD-HHMMSS"""
    now = datetime.now()
    date_part = now.strftime("%Y%m%d")
    time_part = now.strftime("%H%M%S")
    return now, f"{dag_name}__v{date_part}-{time_part}"

@app.get("/")
def read_root():
    return {
        "message": "BI Task Registry API",
        "endpoints": {
            "health_check": "/health",
            "register_task": "/send_task"
        }
    }

@app.get("/health")
def health_check():
    return {"status": "OK"}

@app.get("/ip")
def get_ip():
    if external_IP and external_port :
        return {'ip' : external_IP, 'port' : external_port}
    try:
        ip = requests.get('https://ifconfig.me/ip').text.strip()
        return {'ip' : ip, 'port' : external_port}
    except Exception as e:
        return {"ip": "localhost", "port" : external_port}

@app.post("/send_task")
def register_task(task: TaskCreate, db: Session = Depends(get_db)):
    # Преобразуем metadata в строку для сравнения
    metadata_str = json.dumps(task.task_metadata, sort_keys=True)

    # Проверяем существующие записи
    existing_tasks = db.query(BITaskRegister) \
        .filter(BITaskRegister.dag_name == task.dag_name) \
        .all()

    # Сравниваем metadata как строки
    for existing in existing_tasks:
        existing_metadata_str = json.dumps(existing.task_metadata, sort_keys=True)
        if existing_metadata_str == metadata_str:
            return {'warning' : f"Задача уже зарегистрирована (ID: {existing.id}), статус: {existing.status}"}

    try:
        timestamp, filename = generate_filename(task.dag_name)
        db_task = BITaskRegister(
            dag_name      = task.dag_name,
            task_metadata = task.task_metadata,
            filename      = filename,
            timestamp     = timestamp
        )
        db.add(db_task)
        db.commit()
        db.refresh(db_task)
        logger.info(f"Зарегистрирована новая задача: {db_task.id}")
        return {'message': f"Зарегистрирована новая задача: {db_task.id}"}

    except Exception as e:
        db.rollback()
        logger.error(f"Ошибка: {str(e)}")
        raise ValueError(f"Ошибка сервера")


@app.get("/files")
async def list_files(request: Request, days: int, db: Session = Depends(get_db)):
    """Получить список записей из БД за последние N дней с файлами и URL"""
    try:
        if days <= 0:
            raise HTTPException(
                status_code=400,
                detail="Количество дней должно быть положительным числом"
            )

        # Рассчитываем дату среза
        cutoff_date = (datetime.now() - timedelta(days=days-1)).date()
        logger.info(f"Диапазон дат: {cutoff_date} - {datetime.now().date()}")

        # Получаем все записи из БД
        db_records = db.query(BITaskRegister) \
            .filter(BITaskRegister.timestamp >= cutoff_date) \
            .order_by(BITaskRegister.timestamp.desc())\
            .all()

        # Получаем все файлы из директории
        try:
            all_files = os.listdir("result")
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Папка 'result' не найдена")

        result = []

        for record in db_records:
            # Базовые данные записи (без filename и timestamp)
            record_data = {
                "created" : record.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "name": record.dag_name,
                "status": status_name.get(record.status),
                **record.task_metadata  # Разворачиваем словарь task_metadata в отдельные ключи
                # "data": record.task_metadata
            }


            # Обрабатываем filename только если status == 3
            if record.status == 3 and record.filename:
                # Ищем все файлы, которые начинаются с этого filename
                matched_files = [f for f in all_files if f.startswith(record.filename)]
                if len(matched_files)>0:
                    file = matched_files[0] #берем любой первый файл из списка
                    # Создаем копию базовых данных записи
                    file_record = record_data.copy()

                    # Добавляем информацию о файле
                    file_record.update({
                        "file_format": file.split('.')[-1] if '.' in file else '',
                        "file_name": {file}
                    })
                    result.append(file_record)
                else:
                    # Нет файлов - добавляем запись без URL
                    no_file_record = record_data.copy()
                    no_file_record.update({
                        "file_format": None,
                        "file_name": None
                    })

                    result.append(no_file_record)
            else:
                # status != 3 - добавляем запись с пустым filename
                no_file_record = record_data.copy()
                no_file_record.update({
                    "file_format": None,
                    "file_name": None
                })
                result.append(no_file_record)
        print(result)
        return {
            "days": days,
            "records": result,
            "count": len(result),
            "file_count": sum(1 for r in result if r["file_name"])
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при получении списка файлов: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Ошибка сервера при обработке запроса"
        )