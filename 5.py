import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.svm import SVR
from sklearn.neighbors import KNeighborsRegressor
from sklearn.metrics import r2_score, mean_squared_error

# Load data
data = pd.read_csv("US_graduate_schools_admission_parameters_dataset.csv")

# STRIP all column names to remove spaces
data.columns = data.columns.str.strip()

# Now select features and target
X = data[['GRE Score', 'TOEFL Score', 'University Rating', 'SOP', 'LOR', 'CGPA', 'Research']]
y = data['Chance of Admit']

# Split and scale
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Train Random Forest first (for prediction)
rf_model = RandomForestRegressor(random_state=42)
rf_model.fit(X_train_scaled, y_train)

# Sample prediction
sample = pd.DataFrame([[320, 110, 4, 4.5, 4.0, 9.0, 1]], columns=X.columns)
sample_scaled = scaler.transform(sample)
pred = rf_model.predict(sample_scaled)
print(f"Predicted Admission Chance: {pred[0]*100:.1f}%")

# Train & evaluate other models
models = {
    "Linear Regression": LinearRegression(),
    "Decision Tree": DecisionTreeRegressor(random_state=42),
    "Random Forest": rf_model,  # already trained
    "SVM": SVR(kernel='rbf'),
    "KNN": KNeighborsRegressor(n_neighbors=5)
}

print("\nModel Performance:")
for name, model in models.items():
    if name != "Random Forest":  # RF already trained
        model.fit(X_train_scaled, y_train)
    y_pred = model.predict(X_test_scaled)
    r2 = r2_score(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    print(f"{name:20s} R²: {r2:.3f}  RMSE: {rmse:.3f}")
