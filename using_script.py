import pandas as pd

# Load your Excel file
file_path = "myexcel.xlsx"
df = pd.read_excel(file_path)

# Fill NaN to avoid errors in string operations
df = df.fillna("")

# Step 1: Combine relevant fields for keyword search
search_columns = ['component name', 'Drawing Info', 'Part Name', 'equipment']
df['search_string'] = df[search_columns].astype(str).agg(' '.join, axis=1).str.upper()

# Step 2: Define rules to detect TYPE
type_keywords = {
    'component': ['CYLINDER', 'ROD', 'RING', 'VALVE', 'PISTON', 'CASE', 'COMPRESSOR', 'PIN', 'COVER', 'METAL'],
    'spare': ['SPARE', 'REPLACEMENT', 'STOCK', 'MAINTENANCE', 'STORAGE'],
    'store': ['PAINT', 'CLEANER', 'RAG', 'TOOL', 'BRUSH', 'TAPE', 'LUBRICANT', 'SUPPLY', 'CONSUMABLE']
}

# Default all as unknown
df['type'] = 'unknown'

# Apply type classification
for type_name, keywords in type_keywords.items():
    pattern = '|'.join(keywords)
    mask = df['search_string'].str.contains(pattern, case=False, na=False)
    df.loc[mask, 'type'] = type_name

# Step 3: Categorize components only
df['category'] = ''

# Only focus on component rows for categorization
component_df = df['type'] == 'component'

component_categories = {
    "Ship General": ["REENA", "ACCOMM", "ACCOMODATION", "SHIP GENERAL"],
    "Hull": ["HULL", "PLATE", "WELD", "STRUCTURE"],
    "Equipment For Cargo": ["CARGO", "TANK", "PIPELINE", "VALVE CARGO"],
    "Ship Equipment": ["ANCHOR", "WINCH", "CAPSTAN", "MOORING"],
    "Equipment For Crew And Passengers": ["LADDER", "BED", "SEAT", "TOILET", "SHOWER"],
    "Machinery Main Components": ["CRANK", "PISTON", "ROD", "CYLINDER", "ENGINE", "COMPRESSOR", "PIN METAL"],
    "Systems For Machinery Main Components": ["AIR COOLER", "FUEL SYSTEM", "OIL PUMP", "WATER PUMP"],
    "Ship Common Systems": ["FIRE", "BILGE", "BALLAST", "COMMON", "DRAIN"],
    "HVAC System": ["AIR CONDITIONING", "A/C", "VENTILATION", "BLOWER", "HVAC"]
}

# Apply component category classification
for category, keywords in component_categories.items():
    pattern = '|'.join(keywords)
    mask = component_df & df['search_string'].str.contains(pattern, case=False, na=False)
    df.loc[mask, 'category'] = category

# Set default if category is still blank
df.loc[(df['type'] == 'component') & (df['category'] == ''), 'category'] = 'Uncategorized'

# Clean up helper column
df.drop(columns=['search_string'], inplace=True)

# Save the results
output_path = "classified_myexcel_output.xlsx"
df.to_excel(output_path, index=False)
print(f"✅ Excel file saved as: {output_path}")
