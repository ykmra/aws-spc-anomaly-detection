import awswrangler as wr
import pandas as pd
import numpy as np
import boto3
import json
from datetime import datetime, timedelta, timezone

# ==========================================
# 設定
# ==========================================
BUCKET_NAME = "sample-coating-data"
OUTPUT_PREFIX = "calculation-results"
DATABASE_NAME = "default"
TABLE_NAME = "coating_data"
ATHENA_OUTPUT = f"s3://{BUCKET_NAME}/athena-results/"
# ==========================================

def save_to_s3(results_list, date_str):
    """
    辞書のリストを受け取り、NDJSON形式（1行1JSON）でS3に保存する
    """
    s3 = boto3.client("s3")
    file_key = f"{OUTPUT_PREFIX}/{date_str}.json"
    
    # リスト内の各辞書をJSON文字列に変換し、改行で結合する
    ndjson_body = "\n".join([json.dumps(record, ensure_ascii=False) for record in results_list])
    
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_key,
        Body=ndjson_body,
        ContentType="application/json",
    )
    print(f"Saved: s3://{BUCKET_NAME}/{file_key}")

def lambda_handler(event, context):
    # JST現在時刻
    JST = timezone(timedelta(hours=9))
    now_jst = datetime.now(JST)
    
    # 日付文字列
    date_str = now_jst.strftime("%Y-%m-%d")
    
    print("Start Athena Query...")
    
    # SQLクエリ修正: product_id を追加し、GROUP BY で製品ごとに集計
    sql = f"""
    SELECT 
        product_id,
        AVG(thickness) as mu, 
        STDDEV_SAMP(thickness) as sigma, 
        COUNT(*) as count
    FROM {TABLE_NAME}
    WHERE parse_datetime(timestamp, 'yyyy-MM-dd HH:mm:ss') >= current_timestamp - interval '72' hour
    GROUP BY product_id
    """

    try:
        # Athenaクエリ実行
        df = wr.athena.read_sql_query(
            sql=sql,
            database=DATABASE_NAME,
            s3_output=ATHENA_OUTPUT
        )
        
        print("Query Result Head:", df.head())

        if df.empty:
            print("No data found.")
            return {"message": "No data found"}

        results_list = []

        # DataFrameの各行（各製品）に対して処理を実行
        for index, row in df.iterrows():
            # 値の取り出し
            product_id = str(row['product_id'])
            mu = float(row['mu'])
            sigma = row['sigma']
            count = int(row['count'])
            
            # sigmaがNaN（データ数が1件の場合など）またはNoneの場合は0にする
            if pd.isna(sigma) or sigma is None:
                sigma = 0.0
            else:
                sigma = float(sigma)

            # --- 統計計算 ---
            # 標準偏差が0の場合のゼロ除算対策
            calc_sigma = sigma if sigma > 0 else 1e-6

            # 個別値の管理限界
            ucl_i = mu + 3 * calc_sigma
            lcl_i = mu - 3 * calc_sigma

            # X-bar (n=5) の管理限界
            n = 5
            sigma_xbar = calc_sigma / np.sqrt(n)
            ucl_d = mu + 3 * sigma_xbar
            lcl_d = mu - 3 * sigma_xbar

            # --- 結果作成 ---
            result = {
                "product_id": product_id,
                "date_key": date_str,
                "mean": round(mu, 4),
                "std_dev": round(sigma, 4),
                "ucl_i_d": round(ucl_i, 4),
                "lcl_i_d": round(lcl_i, 4),
                "ucl_d": round(ucl_d, 4),
                "lcl_d": round(lcl_d, 4)
            }
            results_list.append(result)

        # 保存処理（リストごと渡す）
        if results_list:
            save_to_s3(results_list, date_str)
            print(f"Processed {len(results_list)} products.")
        
        return results_list

    except Exception as e:
        print(f"Error: {e}")
        raise e