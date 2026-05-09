import os
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier
import numpy as np
os.makedirs("ml/models", exist_ok=True)

feature_cols = [
    'TransactionAmt','TransactionDT',
    'card1', 'card2', 'card3','card4', 'card5', 'card6',
    'addr1', 'addr2','dist1', 'dist2','P_emaildomain', 'R_emaildomain',
    'C1', 'C2', 'C3', 'C4', 'C5', 'C6','C7', 'C8', 'C9', 'C10', 'C11', 'C13', 'C14',
    'D1','D2', 'D3', 'D4', 'D5', 'D6', 'D10','D11', 'D15',
]
df = pd.read_csv("ml/data/train_transaction.csv")
keep_cols = feature_cols + ['isFraud']
df = df[[c for c in keep_cols if c in df.columns]]

categorical_cols = ['card4', 'card6', 'P_emaildomain', 'R_emaildomain',
                    'M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9']
for col in categorical_cols:
    if col in df.columns:
        le = LabelEncoder()
        df[col] = df[col].fillna('unknown')
        df[col] = le.fit_transform(df[col].astype(str))
df['amt_log'] = np.log1p(df['TransactionAmt'])
df['hour'] = (df['TransactionDT'] / 3600) % 24
df['day']  = (df['TransactionDT'] / (3600 * 24)) % 7 
feature_cols = feature_cols + ['amt_log', 'hour', 'day']
df[feature_cols] = df[feature_cols].fillna(-999)

X = df[feature_cols]
y = df['isFraud']

fraud_rate = y.mean()
print(f"Dataset: {len(df)} transactions, {fraud_rate:.1%} fraud rate")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

print("Training XGBoost model...")
model = XGBClassifier(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    scale_pos_weight=28,
    colsample_bytree=0.8,
    random_state=42,
    n_jobs=-1,
    eval_metric='auc',

)

model.fit(X_train, y_train)

y_pred = model.predict(X_test)
y_prob = model.predict_proba(X_test)[:, 1]
auc = roc_auc_score(y_test, y_prob)

print("\nModel performance:")
print(classification_report(y_test, y_pred, target_names=['legitimate', 'fraud']))
print(f"AUC-ROC: {auc:.4f}")

joblib.dump(model, "ml/models/fraud_model_IEEE.pkl")
joblib.dump(feature_cols, "ml/models/feature_cols_IEEE.pkl")
print("\nModel saved to ml/models/fraud_model_IEEE.pkl")
print("Feature columns saved to ml/models/feature_cols_IEEE.pkl")