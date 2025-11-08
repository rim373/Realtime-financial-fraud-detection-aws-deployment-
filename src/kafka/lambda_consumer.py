import json, boto3, logging, os
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger('k2l')

# Load config from .env
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
LAMBDA_FN = os.getenv('LAMBDA_FN')
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

lambda_client = boto3.client('lambda', region_name='us-east-1')

for msg in consumer:
    LOG.info(f"📩 Received message: {msg.value}")
    response = lambda_client.invoke(
        FunctionName=LAMBDA_FN,
        InvocationType='Event',  # async
        Payload=json.dumps([msg.value])
    )
    LOG.info(f"🚀 Lambda invoked: {response['StatusCode']}")