import time
import os
import json
import datetime
import uuid
import pandas as pd
import boto3
from botocore.exceptions import ClientError

# AWSの設定
AWS_REGION = "ap-northeast-1"
STREAM_NAME = "spc-stream"
# 読み取る対象のデータのディレクトリ
CSV_FOLDER_PATH = r"/Users/yuka/Desktop/sample-data"

# リトライ設定
MAX_RETRIES = 5   # 最大リトライ回数
BASE_DELAY = 0.5  # 初回リトライ時の待機時間(秒)

# クライアント初期化
kinesis_client = boto3.client('kinesis', region_name=AWS_REGION)


def get_todays_csv_path():
    """現在の日付に基づいたファイルパスを返す"""
    today_str = datetime.datetime.now().strftime('%Y%m%d')
    return os.path.join(CSV_FOLDER_PATH, f"sample_data{today_str}.csv")

def get_csv_headers(file_path):
    """
    CSVファイルの先頭行を読み込み、カラム名のリストを返す。
    """
    try:
        df_header = pd.read_csv(file_path, nrows=0, encoding='utf-8')
        return df_header.columns.tolist()
    except Exception as e:
        print(f"Failed to read headers: {e}")
        return []

def send_batch_with_retry(records):
    """
    500件以下のレコードリストを受け取り、リトライ・バックオフ付きでKinesisに送信する
    """
    if not records:
        return

    attempt = 0
    records_to_send = records # 送信対象のレコードリスト

    while len(records_to_send) > 0 and attempt < MAX_RETRIES:
        try:
            # Kinesisへ一括送信
            response = kinesis_client.put_records(
                StreamName=STREAM_NAME,
                Records=records_to_send
            )
            
            failed_count = response['FailedRecordCount']
            
            # 全件成功した場合
            if failed_count == 0:
                if attempt > 0:
                    print(f"Retry successful on attempt {attempt + 1}")
                return

            # 一部失敗した場合の処理
            print(f"Warning: {failed_count} records failed. Retrying... (Attempt {attempt + 1}/{MAX_RETRIES})")
            
            # 失敗したレコードのみを抽出して次回の送信リストを作成
            retry_records = []
            for i, result in enumerate(response['Records']):
                if 'ErrorCode' in result:
                    retry_records.append(records_to_send[i])
            
            records_to_send = retry_records

        except ClientError as e:
            # ネットワークエラーやスロットリングなどの例外発生時
            print(f"Kinesis ClientError: {e}. Retrying batch... (Attempt {attempt + 1}/{MAX_RETRIES})")
        
        except Exception as e:
            print(f"Unexpected Error during send: {e}")
            break

        # バックオフ（待機）処理: 0.5s, 1s, 2s...
        sleep_time = BASE_DELAY * (2 ** attempt)
        time.sleep(sleep_time)
        attempt += 1

    # リトライ上限到達時
    if len(records_to_send) > 0:
        print(f"Critical Error: Failed to send {len(records_to_send)} records after {MAX_RETRIES} attempts.")

def send_to_kinesis(records_list):
    """
    大量のレコードを受け取り、500件ごとのバッチに分割して
    リトライ付き送信関数(send_batch_with_retry)に渡す
    """
    BATCH_SIZE = 500
    for i in range(0, len(records_list), BATCH_SIZE):
        batch = records_list[i:i + BATCH_SIZE]
        send_batch_with_retry(batch)

def main():
    current_file_path = get_todays_csv_path()
    last_pos = 0
    current_columns = [] 

    # 初回起動時の処理
    if os.path.exists(current_file_path) and os.path.getsize(current_file_path) > 0:
        current_columns = get_csv_headers(current_file_path)
        print(f"Columns detected: {current_columns}")
        
        # 既にあるデータの末尾までシーク
        last_pos = os.path.getsize(current_file_path)
        print(f"Monitoring start (tailing): {current_file_path}")
    else:
        print(f"Waiting for file creation: {current_file_path}")

    while True:
        target_file_path = get_todays_csv_path()
        
        # 日付変更などでファイルが切り替わった場合
        if target_file_path != current_file_path:
            if os.path.exists(target_file_path) and os.path.getsize(target_file_path) > 0:
                print(f"New file detected. Switching to: {target_file_path}")
                current_file_path = target_file_path
                last_pos = 0
                current_columns = get_csv_headers(current_file_path)
                print(f"New columns detected: {current_columns}")
            else:
                pass

        if os.path.exists(current_file_path):
            try:
                # ヘッダーがまだ取得できていない場合の再取得試行
                if not current_columns and os.path.getsize(current_file_path) > 0:
                    current_columns = get_csv_headers(current_file_path)

                with open(current_file_path, 'r', encoding='utf-8') as f:
                    # 初回読み込み時にヘッダーをスキップ
                    if last_pos == 0 and current_columns:
                        f.readline() 
                        last_pos = f.tell()
                    
                    f.seek(last_pos)
                    
                    try:
                        if current_columns:
                            # ヘッダー名を使用して読み込み
                            df = pd.read_csv(f, header=None, names=current_columns)
                        else:
                            df = pd.DataFrame()
                    except pd.errors.EmptyDataError:
                        df = pd.DataFrame()
                    
                    last_pos = f.tell()

                    if not df.empty:
                        # 数値変換処理
                        for col in df.columns:
                            try:
                                df[col] = pd.to_numeric(df[col])
                            except (ValueError, TypeError):
                                pass

                        records = df.to_dict(orient='records')
                        kinesis_records = []

                        for data_dict in records:
                            # NaNをNoneに変換
                            for k, v in data_dict.items():
                                if pd.isna(v):
                                    data_dict[k] = None
                            json_str = json.dumps(data_dict, ensure_ascii=False) + '\n'
                            
                            # Kinesis用のレコード形式を作成
                            # PartitionKeyはUUIDを使用（分散効率化）
                            kinesis_records.append({
                                'Data': json_str.encode('utf-8'),
                                'PartitionKey': str(uuid.uuid4())
                            })
                        
                        if len(kinesis_records) > 0:
                            send_to_kinesis(kinesis_records)
                            print(f"Sent {len(df)} rows from {os.path.basename(current_file_path)}")

            except Exception as e:
                print(f"Error in main loop: {e}")
                time.sleep(0.5) # エラー時も少し待機
        time.sleep(0.5)

if __name__ == "__main__":
    main()