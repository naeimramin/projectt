from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, Boolean, Date, Numeric
from sqlalchemy.dialects.postgresql import insert

# تنظیمات پایگاه داده
DATABASE_URL = "postgresql://postgres:your_password@localhost/playstore"
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# تعریف جداول
categories = Table(
    'categories', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, unique=True)
)

developers = Table(
    'developers', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String, unique=True)
)

apps = Table(
    'apps', metadata,
    Column('app_id', String, primary_key=True),
    Column('name', String),
    Column('category_id', Integer),
    Column('developer_id', Integer),
    Column('rating', Float),
    Column('rating_count', Integer),
    Column('installs', Numeric),
    Column('min_installs', Numeric),
    Column('max_installs', Numeric),
    Column('free', Boolean),
    Column('price', Float),
    Column('currency', String),
    Column('size', String),
    Column('min_android', String),
    Column('released', Date),
    Column('last_update', Date),
    Column('content_rating', String),
    Column('privacy_policy', String),
    Column('ad_supported', Boolean),
    Column('in_app_purchases', Boolean),
    Column('editors_choice', Boolean),
    Column('scraped_time', String)
)

# ایجاد اپ FastAPI
app = FastAPI()

# مدل‌های Pydantic
class CategoryBase(BaseModel):
    name: str

class DeveloperBase(BaseModel):
    name: str

class AppBase(BaseModel):
    app_id: str
    name: str
    category_id: int
    developer_id: int
    rating: float | None = None
    rating_count: int | None = None
    installs: float | None = None
    min_installs: float | None = None
    max_installs: float | None = None
    free: bool
    price: float | None = None
    currency: str | None = None
    size: str | None = None
    min_android: str | None = None
    released: str | None = None
    last_update: str | None = None
    content_rating: str | None = None
    privacy_policy: str | None = None
    ad_supported: bool | None = None
    in_app_purchases: bool | None = None
    editors_choice: bool | None = None
    scraped_time: str | None = None

# عملیات CRUD برای دسته‌بندی‌ها
@app.post("/categories/", response_model=CategoryBase)
def create_category(category: CategoryBase):
    with engine.connect() as conn:
        stmt = insert(categories).values(name=category.name).on_conflict_do_nothing(index_elements=['name'])
        conn.execute(stmt)
        return {"name": category.name}

@app.get("/categories/{category_id}", response_model=CategoryBase)
def read_category(category_id: int):
    with engine.connect() as conn:
        result = conn.execute(categories.select().where(categories.c.id == category_id)).fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="Category not found")
        return {"id": result.id, "name": result.name}

@app.put("/categories/{category_id}", response_model=CategoryBase)
def update_category(category_id: int, category: CategoryBase):
    with engine.connect() as conn:
        stmt = categories.update().where(categories.c.id == category_id).values(name=category.name)
        conn.execute(stmt)
        return {"id": category_id, "name": category.name}

@app.delete("/categories/{category_id}")
def delete_category(category_id: int):
    with engine.connect() as conn:
        stmt = categories.delete().where(categories.c.id == category_id)
        conn.execute(stmt)
        return {"detail": "Category deleted"}

# عملیات CRUD برای توسعه‌دهندگان
@app.post("/developers/", response_model=DeveloperBase)
def create_developer(developer: DeveloperBase):
    with engine.connect() as conn:
        stmt = insert(developers).values(name=developer.name).on_conflict_do_nothing(index_elements=['name'])
        conn.execute(stmt)
        return {"name": developer.name}

@app.get("/developers/{developer_id}", response_model=DeveloperBase)
def read_developer(developer_id: int):
    with engine.connect() as conn:
        result = conn.execute(developers.select().where(developers.c.id == developer_id)).fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="Developer not found")
        return {"id": result.id, "name": result.name}

@app.put("/developers/{developer_id}", response_model=DeveloperBase)
def update_developer(developer_id: int, developer: DeveloperBase):
    with engine.connect() as conn:
        stmt = developers.update().where(developers.c.id == developer_id).values(name=developer.name)
        conn.execute(stmt)
        return {"id": developer_id, "name": developer.name}

@app.delete("/developers/{developer_id}")
def delete_developer(developer_id: int):
    with engine.connect() as conn:
        stmt = developers.delete().where(developers.c.id == developer_id)
        conn.execute(stmt)
        return {"detail": "Developer deleted"}

# عملیات CRUD برای اپلیکیشن‌ها
@app.post("/apps/", response_model=AppBase)
def create_app(app: AppBase):
    with engine.connect() as conn:
        stmt = insert(apps).values(app.dict()).on_conflict_do_nothing(index_elements=['app_id'])
        conn.execute(stmt)
        return app

@app.get("/apps/{app_id}", response_model=AppBase)
def read_app(app_id: str):
    with engine.connect() as conn:
        result = conn.execute(apps.select().where(apps.c.app_id == app_id)).fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="App not found")
        return {
            "app_id": result.app_id,
            "name": result.name,
            "category_id": result.category_id,
            "developer_id": result.developer_id,
            "rating": result.rating,
            "rating_count": result.rating_count,
            "installs": result.installs,
            "min_installs": result.min_installs,
            "max_installs": result.max_installs,
            "free": result.free,
            "price": result.price,
            "currency": result.currency,
            "size": result.size,
            "min_android": result.min_android,
            "released": result.released,
            "last_update": result.last_update,
            "content_rating": result.content_rating,
            "privacy_policy": result.privacy_policy,
            "ad_supported": result.ad_supported,
            "in_app_purchases": result.in_app_purchases,
            "editors_choice": result.editors_choice,
            "scraped_time": result.scraped_time
        }

@app.put("/apps/{app_id}", response_model=AppBase)
def update_app(app_id: str, app: AppBase):
    with engine.connect() as conn:
        stmt = apps.update().where(apps.c.app_id == app_id).values(app.dict())
        conn.execute(stmt)
        return app

@app.delete("/apps/{app_id}")
def delete_app(app_id: str):
    with engine.connect() as conn:
        stmt = apps.delete().where(apps.c.app_id == app_id)
        conn.execute(stmt)
        return {"detail": "App deleted"}