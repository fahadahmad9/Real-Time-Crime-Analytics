import pandas as pd

# crimes.csv → 50,000 rows
print("Trimming crimes.csv...")
df = pd.read_csv('crimes.csv', nrows=100000)  # read a bit more to filter
df = df.sample(n=50000, random_state=42)
df.to_csv('crimes.csv', index=False)
print(f"  Done: {len(df)} rows")

# arrests.csv → 10,000 rows
print("Trimming arrests.csv...")
df = pd.read_csv('arrests.csv', nrows=10000)
df.to_csv('arrests.csv', index=False)
print(f"  Done: {len(df)} rows")

# violence.csv → 10,000 rows
print("Trimming violence.csv...")
df = pd.read_csv('violence.csv', nrows=10000)
df.to_csv('violence.csv', index=False)
print(f"  Done: {len(df)} rows")

# police_stations.csv → keep all (only 25 rows, that's fine)
print("police_stations.csv kept as-is (25 rows)")

# sex_offenders.csv → keep all (only 3953 rows, that's fine)
print("sex_offenders.csv kept as-is (3953 rows)")

print("\nAll done!")