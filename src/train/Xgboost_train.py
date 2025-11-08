import pandas as pd
import xgboost as xgb
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
import joblib
import boto3
import os

# Load datasets
train = pd.read_csv('train.csv')
test = pd.read_csv('test.csv')

# Drop TransactionID if it exists
if 'TransactionID' in train.columns:
    train = train.drop(columns=['TransactionID'])
if 'TransactionID' in test.columns:
    test = test.drop(columns=['TransactionID'])

# Split features and labels
X_train = train.drop(columns=['isFraud'])
y_train = train['isFraud']

# If test has no labels (as designed)
if 'isFraud' in test.columns:
    X_test = test.drop(columns=['isFraud'])
    y_test = test['isFraud']
else:
    X_test = test
    y_test = None

# Train model
model = xgb.XGBClassifier(
    n_estimators=100,
    max_depth=6,
    learning_rate=0.1,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    tree_method="hist"  # optimized for CPU
)
model.fit(X_train, y_train)

# Evaluate
if y_test is not None:
    preds = model.predict(X_test)
    print("\n✅ Accuracy:", accuracy_score(y_test, preds))
    print("\nConfusion Matrix:\n", confusion_matrix(y_test, preds))
    print("\nClassification Report:\n", classification_report(y_test, preds))

# Save model locally
os.makedirs("model", exist_ok=True)
joblib.dump(model, "model/xgb_model.pkl")
print("📦 Model saved locally as model/xgb_model.pkl")

# Upload to S3
s3 = boto3.client('s3')
BUCKET = 'financial-fraud-project'
MODEL_KEY = 'xgb/model/xgb_model.pkl'

s3.upload_file("model/xgb_model.pkl", BUCKET, MODEL_KEY)
print(f"☁️ Model uploaded to s3://{BUCKET}/{MODEL_KEY}")
