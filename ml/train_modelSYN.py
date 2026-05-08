import os
import numpy as np
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score


os.makedirs("ml/models", exist_ok=True)
print("Generating synthetic fraud training data...")


np.random.seed(42)
n_samples=50000

data={
    'txn_count_10min': np.random.poisson(2,n_samples),
    'avg_amount_10min':  np.random.exponential(300, n_samples),
    'max_amount_10min':  np.random.exponential(500, n_samples),
    'min_amount_10min':  np.random.exponential(100, n_samples),
    'amount':np.random.exponential(200, n_samples),
    'hour_of_day': np.random.randint(0,24,n_samples),
    'is_casino':np.random.binomial(1,0.05,n_samples),
    'is_new_merchant':   np.random.binomial(1, 0.1, n_samples),

}


df=pd.DataFrame(data)
df['is_fraud'] = 0
mask1=(df['txn_count_10min']>=3) & (df['avg_amount_10min'] >500)
df.loc[mask1,'is_fraud']=1

mask2=(df['is_casino']==1) & (df['amount'] > df['avg_amount_10min']*3)
df.loc[mask2,'is_fraud']=1

mask3=(df['hour_of_day'].between(1,4)) & (df['is_new_merchant']==1) & (df['amount']>1000)
df.loc[mask3,'is_fraud']=1

fraud_rate=df['is_fraud'].mean()
print(f'Dataset:{len(df)} transactions,{fraud_rate:.1%} fraud_rate')

feature_cols=['txn_count_10min','avg_amount_10min','max_amount_10min','min_amount_10min', 'amount', 'hour_of_day', 'is_casino','is_new_merchant']


X=df[feature_cols]
y=df['is_fraud']

X_train, X_test, y_train, y_test=train_test_split(X,y, test_size=0.2, random_state=42)

print('Training Random Forest model...')
model=RandomForestClassifier(
    n_estimators=100,
    max_depth=10,
    random_state=42,
    n_jobs=-1 #to use all cpu core for training

)

model.fit(X_train,y_train)

y_pred=model.predict(X_test)
y_prob=model.predict_proba(X_test)[:,1]
auc=roc_auc_score(y_test,y_prob)

print('\nModel performance')
print(classification_report(y_test,y_pred, target_names=['legitimate','fraud']))
print(f'AUC-ROC:{auc:.4f}')

joblib.dump(model,"ml/models/fraud_model_SYN.pkl")
joblib.dump(feature_cols, "ml/models/feature_cols_SYN.pkl")
print("\nModel saved to ml/models/fraud_model_SYN.pkl")
print("Feature columns saved to ml/models/feature_cols_SYN.pkl")

