import json
import boto3
import base64
import os
from datetime import datetime, timezone, timedelta

# 環境変数からSNSトピックARNを取得
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')

# SNSクライアントの初期化
sns = boto3.client('sns')

# 日本時間のタイムゾーン
JST = timezone(timedelta(hours=9))

def lambda_handler(event, context):
    """
    Kinesis Data Streamsからのレコードを処理し、SNS経由でメール通知
    """
    
    # 環境変数のチェック
    if not SNS_TOPIC_ARN:
        print("エラー: SNS_TOPIC_ARN環境変数が設定されていません")
        return {
            'statusCode': 500,
            'body': json.dumps('環境変数が設定されていません')
        }
    
    for record in event['Records']:
        try:
            # Kinesisレコードのデコード
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            anomaly_data = json.loads(payload)
            
            # メール本文の作成
            message = create_email_message(anomaly_data)
            subject = create_email_subject(anomaly_data)
            
            # SNS経由でメール送信
            response = sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=subject,
                Message=message
            )
            
            print(f"通知送信成功: MessageId={response['MessageId']}")
            
        except Exception as e:
            print(f"エラー発生: {str(e)}")
            print(f"問題のレコード: {record}")
            # エラーが発生しても他のレコードは処理を続ける
            continue
    
    return {
        'statusCode': 200,
        'body': json.dumps('処理完了')
    }

def create_email_subject(data):
    """メールの件名を生成"""
    product_id = data.get('ProductId', '不明')
    return f"【異常検知アラート】製品ID: {product_id}"

def create_email_message(data):
    """メールの本文を生成"""
    
    # データの取得
    anomaly_type = data.get('AnomalyType', '不明')
    target_time = data.get('TargetTime', '不明')
    measured_value = data.get('MeasuredValue', 0)
    upper_limit = data.get('UpperLimit', 0)
    lower_limit = data.get('LowerLimit', 0)
    baseline_ref = data.get('BaselineRef', '不明')
    product_id = data.get('ProductId', '不明')
    
    # 異常の種類を判定
    if measured_value > upper_limit:
        anomaly_detail = f"上限超過（測定値: {measured_value:.2f} > 上限: {upper_limit:.2f}）"
    elif measured_value < lower_limit:
        anomaly_detail = f"下限超過（測定値: {measured_value:.2f} < 下限: {lower_limit:.2f}）"
    else:
        anomaly_detail = "範囲内だが異常と判定"
    
    # メール本文の作成
    message = f"""
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
異常検知アラート
━━━━━━━━━━━━━━━━━━━━━━━━━━━━

【異常情報】
異常タイプ: {anomaly_type}
異常詳細: {anomaly_detail}

【測定データ】
製品ID: {product_id}
検知時刻: {target_time}
測定値: {measured_value:.2f}

【基準値】
上限値: {upper_limit:.2f}
下限値: {lower_limit:.2f}
基準日: {baseline_ref}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━
このメールは自動送信されています。
通知時刻: {datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
    
    return message.strip()