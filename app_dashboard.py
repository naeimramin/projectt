import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import time

# نقشه‌گذاری دسته‌بندی‌های umbrella
category_mapping = {
    "Games": ["Adventure", "Racing", "Puzzle", "Arcade", "Board", "Casual",
              "Card", "Strategy", "Trivia", "Word", "Action", "Simulation",
              "Music", "Role Playing", "Casino"],
    "Tools": ["Tools", "Productivity", "Communication", "Libraries & Demo",
              "Personalization", "Auto & Vehicles"],
    "Education": ["Education", "Educational", "Books & Reference"],
    "Entertainment": ["Photography", "Video Players & Editors", "Music & Audio",
                      "Comics", "Entertainment"],
    "Social": ["Social", "Dating"],
    "Health & Wellness": ["Medical", "Health & Fitness"]
}

# تابع اتصال به پایگاه داده PostgreSQL
def connect_db():
    return psycopg2.connect(
        dbname="playstore",
        user="postgres",
        password="     ",  # جایگزین کردن با رمز عبور واقعی
        host="localhost",
        port="5432"
    )

# بارگذاری داده‌ها از دیتابیس با استفاده از کوئری SQL
def load_data(query, params=[]):
    with connect_db() as conn:
        start_time = time.time()  # زمان شروع اجرای کوئری
        df = pd.read_sql(query, conn, params=params)
        elapsed = time.time() - start_time  # زمان اجرای کوئری
    return df, elapsed

# تنظیمات اولیه داشبورد
st.title("📊 داشبورد گوگل پلی")
st.write("این داشبورد به شما امکان مشاهده و تحلیل داده‌های اپلیکیشن‌های گوگل پلی را می‌دهد.")

# فیلترهای انتخابی برای کاربر
category = st.selectbox("انتخاب دسته‌بندی", ["All"] + list(category_mapping.keys()))
rating = st.slider("انتخاب محدوده امتیاز", 0.0, 5.0, (0.0, 5.0))
price = st.selectbox("انتخاب نوع قیمت", ["All", "Free", "Paid"])
content_rating = st.selectbox("انتخاب رتبه‌بندی محتوا", ["All", "Everyone", "Teen", "Mature 17+"])

# جستجو با استفاده از فیلترهای انتخاب شده
if st.button("جستجو"):
    query = """
        SELECT a.name AS app_name, c.name AS category_name, a.rating, a.price, a.content_rating,
               a.last_update, d.name AS developer_name
        FROM Apps a
        JOIN Categories c ON a.category_id = c.id
        JOIN Developers d ON a.developer_id = d.id
        WHERE a.rating BETWEEN %s AND %s
    """
    params = [rating[0], rating[1]]

    # فیلتر دسته‌بندی
    if category != "All":
        categories_to_fetch = category_mapping.get(category, [])
        if categories_to_fetch:
            query += " AND c.name = ANY(%s)"
            params.append(categories_to_fetch)

    # فیلتر قیمت
    if price == "Free":
        query += " AND a.price = 0"
    elif price == "Paid":
        query += " AND a.price > 0"

    # فیلتر رتبه‌بندی محتوا
    if content_rating != "All":
        query += " AND a.content_rating = %s"
        params.append(content_rating)

    # بارگذاری داده‌ها از دیتابیس
    df, search_time = load_data(query, params)
    st.write(df)
    st.write(f"🔍 زمان اجرای جستجو: {search_time:.4f} ثانیه")

    # جستجوی اپلیکیشن‌های رایگان در دسته‌بندی اجتماعی
    free_social_query = """
        SELECT name FROM Apps 
        WHERE category_id IN (SELECT id FROM Categories WHERE name = ANY(%s)) 
        AND price = 0;
    """
    free_social_apps, social_time = load_data(free_social_query, [["Social", "Dating"]])
    st.subheader("📱 تمامی اپلیکیشن‌های رایگان در دسته‌بندی اجتماعی")
    st.write(free_social_apps)
    st.write(f"⏱️ زمان جستجو: {social_time:.4f} ثانیه")

    # نمایش روند سالانه انتشار و بروزرسانی اپلیکیشن‌ها
    st.subheader("📅 روند سالانه انتشار و بروزرسانی اپلیکیشن‌ها")
    time_category = st.selectbox("انتخاب دسته‌بندی برای روندها", list(category_mapping.keys()))
    subcategories = category_mapping.get(time_category, [])

    last_update_query = """
        SELECT EXTRACT(YEAR FROM a.last_update) AS year, COUNT(*) AS app_count 
        FROM Apps a 
        JOIN Categories c ON a.category_id = c.id 
        WHERE c.name = ANY(%s) 
        GROUP BY year ORDER BY year;
    """
    last_update_df, last_update_time = load_data(last_update_query, [subcategories])

    released_query = """
        SELECT EXTRACT(YEAR FROM a.released) AS year, COUNT(*) AS app_count  
        FROM Apps a 
        JOIN Categories c ON a.category_id = c.id 
        WHERE c.name = ANY(%s) 
        GROUP BY year ORDER BY year;
    """
    released_df, released_time = load_data(released_query, [subcategories])

    # نمایش نمودارهای روند سالانه
    col1, col2 = st.columns(2)
    with col1:
        fig1 = px.line(last_update_df, x="year", y="app_count", title=f"تعداد بروزرسانی‌ها در {time_category}")
        st.plotly_chart(fig1)
    with col2:
        fig2 = px.line(released_df, x="year", y="app_count", title=f"تعداد اپلیکیشن‌های منتشر شده در {time_category}")
        st.plotly_chart(fig2)

    st.write(f"⏱️ زمان اجرای کوئری بروزرسانی: {last_update_time:.4f} ثانیه")
    st.write(f"⏱️ زمان اجرای کوئری انتشار: {released_time:.4f} ثانیه")

    # نمایش میانگین امتیاز بر اساس دسته‌بندی
    st.subheader("⭐ میانگین امتیاز بر اساس دسته‌بندی")
    avg_rating_query = """
        SELECT c.name AS category, AVG(a.rating) AS avg_rating 
        FROM Apps a JOIN Categories c ON a.category_id = c.id 
        GROUP BY c.name ORDER BY avg_rating DESC;
    """
    avg_rating_df, avg_rating_time = load_data(avg_rating_query, [])
    st.bar_chart(avg_rating_df.set_index("category"))
    st.write(f"⏱️ زمان اجرای کوئری میانگین امتیاز: {avg_rating_time:.4f} ثانیه")

    # مقایسه عملکرد جستجو قبل و بعد از ایندکس‌گذاری
    st.subheader("🔍 مقایسه عملکرد جستجو")
    indexed_category = st.selectbox("انتخاب دسته‌بندی ایندکس‌شده", list(category_mapping.keys()))
    subcategories = category_mapping.get(indexed_category, [])

    # جستجوی بدون ایندکس
    category_query = """
        SELECT name FROM Apps 
        WHERE category_id IN (SELECT id FROM Categories WHERE name = ANY(%s))
    """
    no_index_df, no_index_time = load_data(category_query, [subcategories])
    st.write(no_index_df)
    st.write(f"⏱️ زمان جستجو بدون ایندکس: {no_index_time:.4f} ثانیه")

    # جستجوی با ایندکس
    category_indexed_query = """
        SELECT name FROM Apps 
        WHERE category_id IN (SELECT id FROM Categories WHERE name = ANY(%s))
    """
    indexed_df, indexed_time = load_data(category_indexed_query, [subcategories])
    st.write(indexed_df)
    st.write(f"⚡ زمان جستجو با ایندکس: {indexed_time:.4f} ثانیه")

    improvement_category = no_index_time - indexed_time
    st.write(f"💡 بهبود عملکرد جستجو برای دسته‌بندی: {improvement_category:.4f} ثانیه")

    st.markdown(
        "**توضیح:**\n"
        "- اولین کوئری بدون استفاده از ایندکس اجرا می‌شود، در حالی که دومین کوئری از یک ستون ایندکس‌شده استفاده می‌کند.\n"
        "- ایندکس‌ها در جستجوهای روی دیتاست‌های بزرگ کمک زیادی می‌کنند و زمان جستجو را کاهش می‌دهند.\n"
        "- حتی زمانی که ترتیب نمرات مدنظر نباشد، ایندکس‌ها می‌توانند باعث بهبود عملکرد جستجو شوند."
    )
