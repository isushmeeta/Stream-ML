import os
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score

os.makedirs("ml/models", exist_ok=True)

print("Loading IEEE-CIS fraud dataset...")
df = pd.read_csv("ml/data/train_transaction.csv")

feature_cols = [
    'TransactionAmt',
    'card1', 'card2', 'card3', 'card5',
    'addr1', 'addr2',
    'C1', 'C2', 'C6', 'C11', 'C13', 'C14',
    'D1', 'D10', 'D15',
]

df[feature_cols] = df[feature_cols].fillna(-999)

X = df[feature_cols]
y = df['isFraud']

fraud_rate = y.mean()
print(f"Dataset: {len(df)} transactions, {fraud_rate:.1%} fraud rate")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

print("Training Random Forest model...")
model = RandomForestClassifier(
    n_estimators=100,
    max_depth=10,
    random_state=42,
    n_jobs=-1,
    class_weight='balanced'

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