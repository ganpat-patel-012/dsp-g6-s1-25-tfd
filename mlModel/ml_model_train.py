import joblib
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import pandas as pd

df = pd.read_csv('"C:\Users\hp\dsp-g6-s1-25-tfd\data\Clean_Dataset.csv"')
#......................
X = df.drop(columns=['price'])
y = df['price']

print('Preprocessing pipeline')
categorical_features = ['airline', 'source_city', 'departure_time', 'arrival_time', 'destination_city', 'travel_class']
numeric_features = ['duration', 'days_left']

preprocessor = ColumnTransformer(
    transformers=[
        ('num', 'passthrough', numeric_features),
        ('cat', OneHotEncoder(), categorical_features)
    ])

model = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('regressor', RandomForestRegressor(n_estimators=100, random_state=42))
])

print("Train the model..... wait s'il vous plait")
model.fit(X, y)

print('Save the trained model using joblib')
joblib.dump(model, 'flight_price_predictor.pkl')
print('Your pickel is ready to eat!')