import pandas as pd
from collections import defaultdict

# Load your CSV file
file_path = "Fivestar Components Data - items report.CSV.csv"
df = pd.read_csv(file_path)

# Extract relevant columns
components_df = df[['Unnamed: 0', 'Unnamed: 2', 'Unnamed: 3', 'Unnamed: 4', 'Unnamed: 15']].copy()
components_df.columns = ['Item Name', 'Description', 'Detail 1', 'Detail 2', 'Category Hint']

# Clean data
components_df.dropna(subset=['Item Name'], inplace=True)
components_df.drop_duplicates(subset=['Item Name'], inplace=True)

# Define category mappings
category_map = {
    "Ship General": "1",
    "Hull": "2",
    "Equipment For Cargo": "3",
    "Ship Equipment": "4",
    "Equipment For Crew And Passengers": "5",
    "Machinery Main Components": "6",
    "Systems For Machinery Main Components": "7",
    "Ship Common Systems": "8",
    "HVAC System": "9"
}

# Manual classification sample
classification_samples = {
    "AUX. AIR COMPRESSOR SANWA IRON GS3A": "6",
    "AUX. AIR COMPRESSOR YANMAR CY410200": "6",
    "AUX. AIR RESERVOIR HEMMI IRON 0.05 X 30 KG/CM2": "7",
    "AUXILIARY BLOWER H.H.I. TBCB-060F-7526": "9",
    "AUXILIARY BLOWER HYUNDAI MARINE HAA-334/120N, ...": "9",
    "AUXILIARY BLOWER HYUNDAI MARINE HAR-334/80N": "9",
    "AUXILIARY BLOWER OSAKA BLOWER": "9",
    "AUXILIARY BLOWER NISHISHIBA TB-57M": "9",
    "AUXILIARY BLOWER": "9",
    "ACCOMM LADDER": "5"
}

# Apply classification
components_df["Main Category"] = components_df["Item Name"].map(classification_samples)
components_df["Main Category Name"] = components_df["Main Category"].map({v: k for k, v in category_map.items()})

# Filter only classified
classified_df = components_df.dropna(subset=["Main Category"]).copy()

# Assign hierarchical codes
category_counters = defaultdict(lambda: defaultdict(int))
codes = []

for _, row in classified_df.iterrows():
    cat = row["Main Category"]

    # Sub-category code
    category_counters[cat]["level1"] += 1
    level1 = f"{int(cat)}0{category_counters[cat]['level1']}"

    # Sub-sub-category
    category_counters[cat][level1] += 1
    level2 = f"{level1}0{category_counters[cat][level1]}"

    codes.append(level2)

classified_df["Component Code"] = codes

# Export to Excel
output_file = "Classified_Ship_Components.xlsx"
classified_df.to_excel(output_file, index=False)
print(f"✅ Exported to: {output_file}")
