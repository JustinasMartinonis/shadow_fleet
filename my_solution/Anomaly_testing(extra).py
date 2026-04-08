import pandas as pd
df = pd.read_csv("all_anomaly_events.csv")
print(df[df["anomaly"] == "D"].head(10).to_markdown())

# df = pd.read_csv("all_loitering_events.csv")
# print(df.head(50).to_markdown())