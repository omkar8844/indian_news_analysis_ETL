import requests
import pandas as pd
import feedparser
from datetime import datetime
import json
import time
import hashlib
from io import BytesIO
from minio import Minio
import duckdb
import ast
import polars as pl
import s3fs
from transformers import pipeline
from dotenv import load_dotenv
import os
load_dotenv()

minio_endpoint = os.getenv("MINIO_ENDPOINT")
minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")


def extract_rss_feed_data(url,name):
    client = Minio(
    "localhost:9000",          
    access_key=minio_access_key,        
    secret_key=minio_secret_key,   
    secure=False               
    )
    headers = {
    "User-Agent": "MyRSSReader/1.0 (+https://my_project_site_student_learning.com/contact_omkarpawar4488@gmail.com)"
    }
    for attempt in range(3):
        try:
            response=requests.get(url=url,headers=headers,timeout=10)
            response.raise_for_status()
            break
        except requests.RequestException:
            wait_time=2**attempt
            time.sleep(wait_time)
    feed=feedparser.parse(response.content)
    df=pd.json_normalize(feed.entries)
    def generate_id(title, source):
        return hashlib.sha256(f"{title}{source}".encode()).hexdigest()

    df['source'] = name
    df['unique_id'] = df.apply(lambda row: generate_id(row['title'], row['source']), axis=1)
    df=df.drop_duplicates('unique_id',keep='first')
    df = df.astype(str)
    try:
        df=df.drop(columns='published_parsed')
    except:
        pass
    
    df['load_date_time']=datetime.now()
    df['load_date']=datetime.now().date().strftime("%d-%m_%Y")
    return df

def save_parquet_partition(df,path):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_name=f"{path}/part_{ts}.parquet"
        df=df.drop_duplicates("unique_id",keep='first')
        df.to_parquet(
                object_name,
                engine="pyarrow",
                storage_options={
                        "key":minio_access_key,
                        "secret":minio_secret_key,
                        "client_kwargs": {"endpoint_url": "http://localhost:9000"}
                }
        )

def load_rrs_data_in_bronze():
    dict={"Hindustan_Time":"https://www.hindustantimes.com/feeds/rss/india-news/rssfeed.xml",
        "Times_of_India":"https://timesofindia.indiatimes.com/rssfeedstopstories.cms",
        "The_Hindu":"https://www.thehindu.com/feeder/default.rss",
        "Indian_Express":"https://indianexpress.com/feed/",
        "India_Today":"https://www.indiatoday.in/rss/home",
        "NDTV":"https://feeds.feedburner.com/NDTV-LatestNews",
        "News_18":"https://www.news18.com/commonfeeds/v1/eng/rss/india.xml",
        "Firstpost":"https://www.firstpost.com/commonfeeds/v1/mfp/rss/web-stories.xml",
        "DNA":"https://www.dnaindia.com/feeds/india.xml",
        "Scroll":'https://feeds.feedburner.com/ScrollinArticles.rss',
        "Op_India":'https://www.opindia.com/feed/'
        }

    client = Minio(
    "localhost:9000",          
    access_key=minio_access_key,        
    secret_key=minio_secret_key,   
    secure=False               
    )
    con = duckdb.connect()
    con.execute("""
        SET s3_region='us-east-1';
        SET s3_endpoint='localhost:9000';
        SET s3_access_key_id='admin';
        SET s3_secret_access_key='omkarPawar';
        SET s3_url_style='path';
        SET s3_use_ssl=false;
    """)
    today_str=datetime.now().strftime("%Y-%m-%d")
    base_path="s3://news-data-bronze"
    file_exist=any(client.list_objects('news-data-bronze', prefix=f"", recursive=True))


    if file_exist:
        print("Data exist in bronze storage")
        for i,j in dict.items():
            print(f"Processing {i} RSS feed")
            df=extract_rss_feed_data(url=j,name=i)
            print(f"{len(df)} rows extracted")
            new_df = con.execute(f"""
                    SELECT *
                    FROM df
                    WHERE unique_id NOT IN (
                        SELECT unique_id
                        FROM read_parquet('{base_path}/*/*/*.parquet')
                    )
                    """).fetch_df()
            
            if not new_df.empty:
                    print(f"Out of {len(df)} row {len(new_df)} rows selected")
                    save_parquet_partition(df=new_df,path=f"{base_path}/load_date={today_str}/source={i}")
                    print(f"{len(new_df)} rows appended")
            else:
                    print("No new rows to append")
    else:
        print("No data in bronze storage loading new data")
        for i,j in dict.items():
            print(f"Processing {i} RSS feed")
            df=extract_rss_feed_data(url=j,name=i)
            save_parquet_partition(df=df,path=f"{base_path}/load_date={today_str}/source={i}")
    con.close()


def transform_df_silver(df):
    df["pub_date"]=pd.to_datetime(df['published'],format='mixed')
    df['load_date']=pd.to_datetime(df['load_date'])
    df['load_date_time']=pd.to_datetime(df['load_date_time'])
    #df['pub_time']=df['pub_date'].dt.time
    df.loc[df['tags'].isna(),'tags']='No Tag'
    
    try:
        def extract_terms_split(x):
            if pd.isna(x):
                return None, None
            try:
                parsed = ast.literal_eval(x)
                if isinstance(parsed, list):
                    terms = [t.get('term') for t in parsed if isinstance(t, dict) and 'term' in t]
                    term1 = terms[0] if len(terms) > 0 else None
                    term2 = terms[1] if len(terms) > 1 else None
                    return term1, term2
                return None, None
            except (ValueError, SyntaxError):
                return None, None
        df[['term1', 'term2']] = df['tags'].apply(lambda x: pd.Series(extract_terms_split(x)))
    except:
        pass
    return df

def load_to_silver():
    client = Minio(
    "localhost:9000",          
    access_key=minio_access_key,        
    secret_key=minio_secret_key,   
    secure=False               
    )
    con = duckdb.connect()
    con.execute("""
        SET s3_region='us-east-1';
        SET s3_endpoint='localhost:9000';
        SET s3_access_key_id='admin';
        SET s3_secret_access_key='omkarPawar';
        SET s3_url_style='path';
        SET s3_use_ssl=false;
    """)
    bronze_path="s3://news-data-bronze"
    silver_path="s3://news-data-silver"
    columns = [
        'author',  
        'media_content',
        'tags',
        'id',
        'load_date_time',
        'load_date',
        'title',
        'unique_id',
        'source',
        'summary',
        'published',
        'link'
    ]
    col_str = ", ".join(columns)
    file_exist=any(client.list_objects('news-data-silver', prefix=f"", recursive=True))
    today_str=datetime.now().strftime("%Y-%m-%d")
    if file_exist:
        print("Transformed data already exist in silver")
        df=con.execute(
            f""" 
                    SELECT {col_str} FROM 
                    READ_PARQUET('{bronze_path}/*/*/*parquet',union_by_name=True) 
                    where unique_id not in (SELECT unique_id FROM
                                            READ_PARQUET('{silver_path}/*/*.parquet')    )
            """
            ).fetch_df()
        if not df.empty:
            print(f"{len(df)} rows selected")
            new_df=transform_df_silver(df)
            save_parquet_partition(df=new_df,path=f"{silver_path}/transform_date={today_str}")
        else:
            print("No new Data")
    else:
        print("No previous data in silver")
        df=con.execute(
            f""" 
                    SELECT {col_str} FROM 
                    READ_PARQUET('{bronze_path}/*/*/*parquet',union_by_name=True)
            """
            ).fetch_df()
        new_df=transform_df_silver(df)
        print(f"{len(new_df)} rows loaded")
        save_parquet_partition(df=new_df,path=f"{silver_path}/transform_date={today_str}")
    con.close()


def transform_to_gold(df):
    # df = df.with_columns([
    #     pl.col("published")
    #     .str.strptime(pl.Datetime, format=None, strict=False)   # published is string → datetime
    #     .dt.convert_time_zone("UTC")
    #     .alias("pub_date"),

    #     pl.col("load_date")
    #     .cast(pl.Date)   # already datetime → cast to Date
    #     .alias("load_date"),

    #     pl.col("load_date_time")
    #     .cast(pl.Datetime)   # already datetime → keep as Datetime
    #     .alias("load_date_time"),
    # ])

    # extract only time from pub_date
    # df = df.with_columns(
    #     df["pub_date"].dt.time().alias("pub_time")
    # )

# drop original 'published' column

    df = df.with_columns(
        pl.when(
            (pl.col("summary") == "") | pl.col("summary").str.contains("href")
        )
        .then(pl.col("title"))
        .otherwise(pl.col("summary"))
        .alias("text_for_analysis")
    )
    texts=df['text_for_analysis'].to_list()
    finbert = pipeline(
        "sentiment-analysis", 
        model="yiyanghkust/finbert-tone"
    )
    results=finbert(texts)
    df=df.hstack(
        pl.DataFrame({
            "sentiment": [i["label"] for i in results],
            "score": [i["score"] for i in results]
        })
    )
    df=df.to_pandas()
    return df

def load_to_gold():
    con = duckdb.connect()
    con.execute("""
        SET s3_region='us-east-1';
        SET s3_endpoint='localhost:9000';
        SET s3_access_key_id='admin';
        SET s3_secret_access_key='omkarPawar';
        SET s3_url_style='path';
        SET s3_use_ssl=false;
    """)
    client = Minio(
    "localhost:9000",          
    access_key=minio_access_key,        
    secret_key=minio_secret_key,   
    secure=False               
    )
    silver_path="s3://news-data-silver"
    gold_path="s3://news-data-gold"
    fs = s3fs.S3FileSystem(
        key=minio_access_key,
        secret=minio_secret_key,
        client_kwargs={"endpoint_url": "http://localhost:9000"}
    )
    files = [
        f"s3://{path}"
        for path in fs.glob("news-data-silver/*/*.parquet")
    ]
    today_str=datetime.now().strftime("%Y-%m-%d")

    file_exist=any(client.list_objects('news-data-gold', prefix=f"", recursive=True))
    if file_exist:
        print("Files exist in gold")
        new_df=con.execute(
            f""" 
                    SELECT * FROM 
                    READ_PARQUET('{silver_path}/*/*.parquet') 
                    where unique_id not in (SELECT unique_id FROM
                                            READ_PARQUET('{gold_path}/*/*.parquet')    )
            """
            ).fetch_df()
        if not new_df.empty:
            new_df = pl.from_pandas(new_df)
            new_df2=transform_to_gold(new_df)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            save_parquet_partition(df=new_df2,path=f"{gold_path}/transform_date={today_str}")
            # new_df2.write_parquet(
            #     f"{gold_path}/load_date={today_str}/part_{ts}.parquet",
            #     storage_options={
            #         "AWS_ACCESS_KEY_ID": minio_access_key,
            #         "AWS_SECRET_ACCESS_KEY": minio_secret_key,
            #         "AWS_REGION": "us-east-1",
            #         "AWS_ENDPOINT_URL": "http://localhost:9000"
            # }
            # )   
        else:
            print("No new data")
    else:
        df=con.execute(
            f""" 
                    SELECT * FROM 
                    READ_PARQUET('{silver_path}/*/*.parquet')
            """
            ).fetch_df()
        df = pl.from_pandas(df)
        print(f"No Data exist in gold storage,loading {len(df)} rows")
        new_df=transform_to_gold(df)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_parquet_partition(df=new_df,path=f"{gold_path}/transform_date={today_str}")
    #     new_df.write_parquet(
    #         f"{gold_path}/load_date={today_str}/part_{ts}.parquet",
    #         storage_options={
    #             "AWS_ACCESS_KEY_ID": minio_access_key,
    #             "AWS_SECRET_ACCESS_KEY": minio_secret_key,
    #             "AWS_REGION": "us-east-1",
    #             "AWS_ENDPOINT_URL": "http://localhost:9000"
    #     }
    # )
    con.close()