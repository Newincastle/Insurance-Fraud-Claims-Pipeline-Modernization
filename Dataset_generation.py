#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd
import numpy as np
import random
import json
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

def introduce_missing(data, col, frac=0.05):
    n = int(len(data) * frac)
    idx = np.random.choice(data.index, n, replace=False)
    data.loc[idx, col] = None

def generate_claims(n=100000):
    df = pd.DataFrame({
        "claim_id": [f"CLM{100000+i}" for i in range(n)],
        "policy_id": [f"POL{10000 + i%50000}" for i in range(n)],  # duplicates
        "customer_id": [f"CUST{10000 + i%40000}" for i in range(n)],
        "agent_id": [f"AGT{10000 + i%25000}" for i in range(n)],
        "claim_amount": np.random.normal(5000, 1500, n).clip(100, 50000),
        "claim_date": [fake.date_between(start_date='-2y', end_date='today') for _ in range(n)],
        "status": np.random.choice(["approved", "pending", "rejected"], n),
        "incident_id": [f"INC{10000 + i%30000}" for i in range(n)],
        "location": [fake.city() for _ in range(n)],
        "channel": np.random.choice(["online", "agent", "branch"], n)
    })
    introduce_missing(df, "claim_amount")
    return df

def generate_customers(n=40000):
    data = []
    for i in range(n):
        profile = {
            "customer_id": f"CUST{10000 + i}",
            "name": fake.name(),
            "dob": fake.date_of_birth(minimum_age=18, maximum_age=80).isoformat(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "income": random.choice([None, round(random.uniform(20000, 150000), 2)]),
            "credit_score": random.choice([None, random.randint(300, 850)]),
            "address": fake.address().replace("\n", ", "),
            "employment_status": random.choice(["employed", "unemployed", "retired"]),
            "marital_status": random.choice(["single", "married", "divorced"])
        }
        data.append(profile)
    return data

def generate_incidents(n=30000):
    df = pd.DataFrame({
        "incident_id": [f"INC{10000 + i}" for i in range(n)],
        "policy_id": [f"POL{10000 + i%50000}" for i in range(n)],
        "category": np.random.choice(["theft", "accident", "fire", "natural disaster", "vandalism"], n),
        "description": [fake.text(max_nb_chars=100) for _ in range(n)],
        "incident_date": [fake.date_between(start_date='-3y', end_date='today') for _ in range(n)],
        "location": [fake.city() for _ in range(n)],
        "reported_by": [fake.name() for _ in range(n)],
        "severity": np.random.choice(["low", "medium", "high"], n),
        "police_report": np.random.choice(["yes", "no"], n),
        "witness_count": np.random.randint(0, 5, n)
    })
    introduce_missing(df, "description")
    return df

def generate_fraud_signals(n=20000):
    df = pd.DataFrame({
        "claim_id": [f"CLM{100000 + i}" for i in range(n)],
        "suspicious_score": np.random.rand(n),
        "is_fraud": np.random.choice([0, 1], n, p=[0.95, 0.05]),
        "flagged_by": np.random.choice(["system", "manual", "audit"], n),
        "review_status": np.random.choice(["pending", "cleared", "confirmed"], n),
        "review_date": [fake.date_between(start_date='-1y', end_date='today') for _ in range(n)],
        "reviewer": [fake.name() for _ in range(n)],
        "notes": [fake.sentence() for _ in range(n)],
        "risk_level": np.random.choice(["low", "medium", "high"], n),
        "escalated": np.random.choice(["yes", "no"], n)
    })
    return df

def generate_agents(n=25000):
    df = pd.DataFrame({
        "agent_id": [f"AGT{10000 + i}" for i in range(n)],
        "name": [fake.name() for _ in range(n)],
        "region": [fake.city() for _ in range(n)],
        "performance_score": np.random.normal(75, 10, n).clip(0, 100),
        "tenure_years": np.random.randint(1, 20, n),
        "active": np.random.choice(["yes", "no"], n),
        "email": [fake.email() for _ in range(n)],
        "phone": [fake.phone_number() for _ in range(n)],
        "supervisor": [fake.name() for _ in range(n)],
        "branch": [fake.city() for _ in range(n)]
    })
    introduce_missing(df, "performance_score")
    return df

# Save datasets
generate_claims().to_csv("claims.csv", index=False)
with open("customer_profiles.json", "w") as f:
    json.dump(generate_customers(), f, indent=2)
generate_incidents().to_csv("incident_reports.csv", index=False)
generate_fraud_signals().to_csv("fraud_signals.csv", index=False)
generate_agents().to_csv("agents.csv", index=False)

print("✅ Datasets generated successfully.")


# In[2]:


pip install faker


# In[9]:


import duckdb
result = duckdb.query("SELECT policy_id, COUNT(*) FROM '/home/user/insurance-project/claims.csv' GROUP BY policy_id HAVING COUNT(*) > 1").df()
print(result)


# In[5]:


pip install duckdb


# In[12]:


import pandas as pd

# Load the CSV file (update path if needed)
df = pd.read_csv("/home/user/insurance-project/claims.csv")

# Count frequency of each policy_number
policy_counts = df['policy_id'].value_counts()

# Display the result
print(policy_counts)


# In[17]:


import pandas as pd

# Load the CSV file
df = pd.read_csv("/home/user/insurance-project/claims.csv")

# Print the number of rows
print("Row count:", len(df))


# In[1]:


import pandas as pd
from scipy.stats import zscore

# Load the CSV file
df = pd.read_csv("/home/user/insurance-project/claims.csv")

# Ensure 'claim_amount' is numeric and drop missing values
df['claim_amount'] = pd.to_numeric(df['claim_amount'], errors='coerce')
df_clean = df.dropna(subset=['claim_amount'])

# Calculate z-score
df_clean['claim_amount_zscore'] = zscore(df_clean['claim_amount'])

# Identify outliers (absolute z-score > 3)
outliers = df_clean[df_clean['claim_amount_zscore'].abs() > 3]

# Print results
print(f"Number of outliers: {len(outliers)}")
print(outliers[['claim_id', 'claim_amount', 'claim_amount_zscore']].head())


# In[19]:


pip install --upgrade numpy scipy pandas


# In[ ]:




