import pandas as pd

df = pd.read_csv("./meta-analysis.csv")
df["PMCID"].str.lstrip("PMC").astype(int).to_csv(
    "./pmcids.txt", index=False, header=None
)
