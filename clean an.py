import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# بارگذاری داده‌ها از فایل CSV
df = pd.read_csv("Google-Playstore.csv")

# حذف رکوردهای تکراری
df.drop_duplicates(inplace=True)

# حذف مقادیر خالی در ستون 'App Id'
df = df.dropna(subset=['App Id'])

# پاکسازی ستون 'Price'
df['Price'] = df['Price'].replace({'\\$': ''}, regex=True).astype(float)

# پاکسازی رشته‌های متنی
df['Category'] = df['Category'].str.strip().str.title()
df['Developer Id'] = df['Developer Id'].str.strip().str.title()
df['Content Rating'] = df['Content Rating'].str.strip()

# تبدیل مقادیر به عددی
df['Rating'] = pd.to_numeric(df['Rating'], errors='coerce')
df['Rating Count'] = pd.to_numeric(df['Rating Count'], errors='coerce')
df['Installs'] = pd.to_numeric(df['Installs'], errors='coerce')
df['Minimum Installs'] = pd.to_numeric(df['Minimum Installs'], errors='coerce')
df['Maximum Installs'] = pd.to_numeric(df['Maximum Installs'], errors='coerce')

# تبدیل 'NaT' به None و تغییر فرمت تاریخ
df['Last Updated'] = pd.to_datetime(df['Last Updated'], errors='coerce')
df['Released'] = pd.to_datetime(df['Released'], errors='coerce')

# مدیریت مقادیر بسیار بزرگ برای جلوگیری از خطای bigint
bigint_max = 9223372036854775807
df['Minimum Installs'] = df['Minimum Installs'].apply(lambda x: x if x < bigint_max else None)
df['Maximum Installs'] = df['Maximum Installs'].apply(lambda x: x if x < bigint_max else None)
df['Rating Count'] = df['Rating Count'].apply(lambda x: x if x < bigint_max else None)


def connect_db():
    return psycopg2.connect(
        dbname="playstore",
        user="postgres",
        password="     ",  # جایگزین کنید
        host="localhost",
        port="5432"
    )


def insert_data(df):
    conn = connect_db()
    cursor = conn.cursor()

    # وارد کردن دسته‌ها
    categories = df['Category'].unique()
    for category in categories:
        cursor.execute("INSERT INTO categories (name) VALUES (%s) ON CONFLICT (name) DO NOTHING;", (category,))

    # وارد کردن توسعه‌دهندگان
    developers = df['Developer Id'].unique()
    developer_values = [(developer,) for developer in developers]
    cursor.executemany("INSERT INTO developers (name) VALUES (%s) ON CONFLICT (name) DO NOTHING;", developer_values)

    conn.commit()

    # بازیابی شناسه دسته‌ها و توسعه‌دهندگان
    cursor.execute("SELECT id, name FROM categories")
    category_dict = {name: id for id, name in cursor.fetchall()}

    cursor.execute("SELECT id, name FROM developers")
    developer_dict = {name: id for id, name in cursor.fetchall()}

    # آماده‌سازی داده‌ها برای درج
    apps_data = []
    for _, row in df.iterrows():
        category_id = category_dict.get(row['Category'])
        developer_id = developer_dict.get(row['Developer Id'])

        if category_id and developer_id:
            apps_data.append((
                row['App Id'],
                row['App Name'],
                category_id,
                developer_id,
                row['Rating'] if pd.notna(row['Rating']) else None,
                row['Rating Count'] if pd.notna(row['Rating Count']) else None,
                row['Installs'] if pd.notna(row['Installs']) else None,
                row['Minimum Installs'] if pd.notna(row['Minimum Installs']) else None,
                row['Maximum Installs'] if pd.notna(row['Maximum Installs']) else None,
                row['Free'],
                row['Price'],
                row['Currency'],
                row['Size'],
                row['Minimum Android'],
                row['Released'] if pd.notna(row['Released']) else None,
                row['Last Updated'] if pd.notna(row['Last Updated']) else None,
                row['Content Rating'],
                row['Privacy Policy'],
                row['Ad Supported'],
                row['In App Purchases'],
                row['Editors Choice'],
                row['Scraped Time']
            ))

    cursor.executemany("""
        INSERT INTO apps (app_id, name, category_id, developer_id, rating, rating_count, installs, min_installs,
        max_installs, free, price, currency, size, min_android, released, last_update, content_rating,
        privacy_policy, ad_supported, in_app_purchases, editors_choice, scraped_time)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (app_id) DO NOTHING;
    """, apps_data)

    conn.commit()
    cursor.close()
    conn.close()


insert_data(df)
