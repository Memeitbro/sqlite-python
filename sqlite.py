import sqlite3
import random
from datetime import date, timedelta # I really hope using these two is fine, considering the assignment.



con = sqlite3.connect('my.db')
cur = con.cursor()

# Task 1: Generate product ratings

def task1():
    create_ratings()
    generate_rows()
    cur.execute("""
        SELECT * FROM Ratings
        """)
    print(cur.fetchall())
    print("")

def create_ratings():
    # every time we try to execute we get new values only
    cur.execute("""
        DROP TABLE IF EXISTS
            Ratings
        """)
    con.commit()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS
            Ratings(
                timestamp text,
                user_id integer,
                product_id integer,
                rating integer
            )
        """)
    con.commit()
    
def generate_rows():
    # this one is quite slow...
    for _ in range(100000):
        time = generate_date()
        cur.execute(f"""
            INSERT INTO
                Ratings
            VALUES(
                '{str(time)}',
                {random.randint(1, 1000)},
                {random.randint(1, 1000)},
                {random.randint(1, 5)}
                )
            """)
        con.commit()
        
def generate_date():
    date_start = date(2024, 1, 1)
    if date_start.year % 4 == 0 and (date_start.year % 100 != 0 or date_start.year % 400 == 0):
        return date_start + timedelta(days=random.randint(0, 366))
    return date_start + timedelta(days=random.randint(0, 365))

# Task 2: Compute monthly aggregates

def task2():
    create_aggregates()
    fill_aggregates()
    cur.execute("""
        SELECT * FROM RatingsMonthlyAggregates
        """)
    print(cur.fetchall())
    print("")
    
def create_aggregates():
    # every time we try to execute we get new values only
    cur.execute("""
        DROP TABLE IF EXISTS
            RatingsMonthlyAggregates
        """)
    con.commit()
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS
            RatingsMonthlyAggregates(
                month integer,
                product_id integer,
                average_rating real
            )
        """)
    con.commit()
    
def fill_aggregates():
    # https://www.sqlite.org/lang_datefunc.html handy :)
    cur.execute("""
    INSERT INTO
        RatingsMonthlyAggregates (month, product_id, average_rating)
    SELECT 
        strftime('%m', timestamp) AS month, 
        product_id,
        AVG(rating) AS average_rating
    FROM 
        Ratings
    GROUP BY 
        month, product_id
    """)
    
    con.commit()
    
# Task 3: Find most successful products for each month

def task3():
    # https://www.sqlitetutorial.net/sqlite-window-functions/sqlite-rank/
    # https://sqlite.org/windowfunctions.html
    # this one... took a lot longer to figure out than I initially expected it to.
    cur.execute("""
    SELECT
        *
    FROM
        (
        SELECT
            month,
            product_id,
            average_rating,
            RANK() OVER ( PARTITION BY month ORDER BY average_rating DESC ) rank
        FROM
            RatingsMonthlyAggregates
        )
    WHERE
        rank <= 3
    ORDER BY
        month, rank
    """)
    print(cur.fetchall())
        

print("we start here")
print("Generate product ratings:")
task1()
print("Compute monthly aggregates:")
task2()
print("Find most successful products for each month:")
task3()

con.close()