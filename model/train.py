import pandas as pd
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
import joblib

# Get correct base directory
base_dir = os.path.dirname(os.path.dirname(__file__))

# Correct dataset path
file_path = os.path.join(base_dir, "data", "fraud.csv")

print("Looking for file at:", file_path)

# Check if file exists
if not os.path.exists(file_path):
    print("❌ Dataset not found!")
    exit(1)

# Load dataset
df = pd.read_csv(file_path)

# Prepare data
X = df.drop("Class", axis=1)
y = df["Class"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

# Train model
model = RandomForestClassifier()
model.fit(X_train, y_train)

# Save model
model_path = os.path.join(base_dir, "model", "model.pkl")
joblib.dump(model, model_path)

print("✅ Model trained & saved at:", model_path)