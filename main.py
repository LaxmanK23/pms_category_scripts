import pandas as pd
import google.generativeai as genai
import concurrent.futures
import time
import os
import getpass

api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    print("🔑 Gemini API key not found in environment.")
    api_key = getpass.getpass("Please enter your Gemini API key (input hidden): ")
genai.configure(api_key=api_key)
model = genai.GenerativeModel("gemini-2.5-pro-exp-03-25")

# Load Excel
df = pd.read_excel("myexcel.xlsx").fillna("")
df['type'] = ''
df['category'] = ''
df['id'] = ''

# ID Mapping
category_id_map = {
    "Ship General": 1,
    "Hull": 2,
    "Equipment For Cargo": 3,
    "Ship Equipment": 4,
    "Equipment For Crew And Passengers": 5,
    "Machinery Main Components": 6,
    "Systems For Machinery Main Components": 7,
    "Ship Common Systems": 8,
    "HVAC System": 9
}

# Prompt builder
def make_prompt(row):
    return f"""
You are a classification model for ship part inventory.

Each part should be labeled with:
- **type**: component, spare, or store
- **category**: choose *one only* from the list below.

CATEGORIES:
1. Ship General
2. Hull
3. Equipment For Cargo
4. Ship Equipment
5. Equipment For Crew And Passengers
6. Machinery Main Components
7. Systems For Machinery Main Components
8. Ship Common Systems
9. HVAC System

Now classify this part:

Component Name: {row['component name']}
Drawing Info: {row['Drawing Info']}
Part Name: {row['Part Name']}
Equipment: {row['equipment']}

Answer format:
type: <component | spare | store>
category: <choose from list above>
"""
subset = df
def classify_row(index, row):
    result = {'index': index, 'type': 'error', 'category': 'error'}
    try:
        prompt = make_prompt(row)
        response = model.generate_content(prompt)
        output = response.text.strip()
        for line in output.split('\n'):
            if "type:" in line.lower():
                result['type'] = line.split(":")[-1].strip().lower()
            elif "category:" in line.lower():
                result['category'] = line.split(":")[-1].strip()
        print(f"✅ Processed row {index + 1}")
    except Exception as e:
        print(f"❌ Error on row {index}: {e}")
    time.sleep(1) 
    return result

results = []
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(classify_row, i, row) for i, row in subset.iterrows()]
    for future in concurrent.futures.as_completed(futures):
        results.append(future.result())
for res in results:
    df.at[res['index'], 'type'] = res['type']
    df.at[res['index'], 'category'] = res['category']
type_counters = {}
part_counters = {}
for i, row in subset.iterrows():
    cat = row['category']
    typ = row['type']
    cat_id = category_id_map.get(cat, 0)
    if cat not in type_counters:
        type_counters[cat] = {}
    if typ not in type_counters[cat]:
        type_counters[cat][typ] = len(type_counters[cat]) + 10
    type_id = type_counters[cat][typ]
    key = f"{cat}-{typ}"
    if key not in part_counters:
        part_counters[key] = 100
    else:
        part_counters[key] += 1
    part_id = part_counters[key]
    df.at[i, 'id'] = f"{cat_id}.{type_id}.{part_id}"
try:
    df.to_excel("classified_by_gemini.xlsx", index=False)
    print("\n✅ Done! Saved as classified_by_gemini.xlsx")
except PermissionError:
    print("\n⚠️ Excel file is open. Please close and run again.")
