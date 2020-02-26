from sqlalchemy import create_engine
from sqlalchemy import Column, Date, Float, Index, MetaData, String, Table, Text
from sqlalchemy.ext.declarative import declarative_base
import pandas as pd

# create username with encrypted password first, then grant database to that username
engine = create_engine("postgresql://lora:lora@localhost/stockmarket")


result = pd.read_sql_query(
    """SELECT day, AVG(price) as avg_hk, MAX(price) as max_hk, MIN(price) as min_hk FROM (SELECT * FROM imported_closes INNER JOIN monthly_members ON monthly_members.month=imported_closes.day WHERE index='HK') AS price  GROUP by day ORDER BY day """,
    engine,
)
# get fifith lowest
result_5_lowest = pd.read_sql_query(
    """SELECT day, MAX(nth_value) as fifth_lowest_kospi2 from (SELECT day, price, NTH_VALUE(price, 5) OVER (PARTITION BY day ORDER BY day, price ASC) FROM (SELECT * FROM imported_closes INNER JOIN monthly_members ON monthly_members.month=imported_closes.day WHERE index='KOSPI2') AS price GROUP BY day, price ORDER BY day) AS nth_value GROUP BY day """,
    engine,
)
# get fifth highest
result_5_highest = pd.read_sql_query(
    """SELECT day, MAX(nth_value) as fifth_highest_kospi2 from (SELECT day, price, NTH_VALUE(price, 5) OVER (PARTITION BY day ORDER BY day, price DESC) FROM (SELECT * FROM imported_closes INNER JOIN monthly_members ON monthly_members.month=imported_closes.day WHERE index='KOSPI2') AS price GROUP BY day, price ORDER BY day) AS nth_value GROUP BY day """,
    engine,
)

df1 = pd.DataFrame(result)
df2 = pd.DataFrame(result_5_highest)
df3 = pd.DataFrame(result_5_lowest)

final = pd.concat([df1, df2, df3], axis=1)
print(final)

final.to_csv('loratech_python.csv', mode='a', header=True)