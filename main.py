import pandas as pd
import google.generativeai as genai
import concurrent.futures
import time
import os
import getpass

# STEP 1: API Setup
api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    print("🔑 Gemini API key not found in environment.")
    api_key = getpass.getpass("Please enter your Gemini API key (input hidden): ")

genai.configure(api_key=api_key)
# model = genai.GenerativeModel("models/gemini-pro")  # <-- Supported for now

# model = genai.GenerativeModel("gemini-2.5-pro-exp-03-25")
model = genai.GenerativeModel("gemini-2.0-flash")
# STEP 2: Load Excel
df = pd.read_excel("myexcel.xlsx").fillna("")
df['type'] = ''
df['category'] = ''
df['id'] = ''

# STEP 3: ID Mapping
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

# STEP 4: Prompt Builder for 10 rows
def make_prompt(rows):
    prompt = """
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

Now classify the following parts:
"""
    for idx, row in enumerate(rows, start=1):
        prompt += f"""
Part {idx}:
Component Name: {row['component name']}
Drawing Info: {row['Drawing Info']}
Part Name: {row['Part Name']}
Equipment: {row['equipment']}
"""
    prompt += "\nReply in this format:\nPart 1:\ntype: <component | spare | store>\ncategory: <from list>\n"
    return prompt

# STEP 5: Classify a chunk
def classify_chunk(start_idx, rows):
    results = []
    try:
        prompt = make_prompt(rows)
        response = model.generate_content(prompt)
        output = response.text.strip()

        parts = output.split("Part ")[1:]  # Skip intro
        for i, part_output in enumerate(parts):
            type_val = "error"
            category_val = "error"
            for line in part_output.strip().split('\n'):
                if "type:" in line.lower():
                    type_val = line.split(":")[-1].strip().lower()
                elif "category:" in line.lower():
                    category_val = line.split(":")[-1].strip()
            results.append({
                'index': start_idx + i,
                'type': type_val,
                'category': category_val
            })

        print(f"✅ Processed rows {start_idx + 1} to {start_idx + len(rows)}")
    except Exception as e:
        print(f"❌ Error on rows {start_idx + 1}-{start_idx + len(rows)}: {e}")
        for i in range(len(rows)):
            results.append({
                'index': start_idx + i,
                'type': 'error',
                'category': 'error'
            })

    time.sleep(1)  # Anti-rate-limit delay
    return results

# STEP 6: Run in parallel (multi-threading)
chunk_size = 100
all_results = []

with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = []
    for start in range(0, len(df), chunk_size):
        chunk_rows = df.iloc[start:start + chunk_size].to_dict(orient='records')
        futures.append(executor.submit(classify_chunk, start, chunk_rows))

    for future in concurrent.futures.as_completed(futures):
        all_results.extend(future.result())

# STEP 7: Update DataFrame
for res in all_results:
    df.at[res['index'], 'type'] = res['type']
    df.at[res['index'], 'category'] = res['category']

# STEP 8: Hierarchical ID Generation
type_counters = {}
part_counters = {}

for i, row in df.iterrows():
    cat = row['category']
    typ = row['type']
    cat_id = category_id_map.get(cat, 0)

    if cat not in type_counters:
        type_counters[cat] = {}
    if typ not in type_counters[cat]:
        type_counters[cat][typ] = len(type_counters[cat]) + 100
    type_id = type_counters[cat][typ]

    key = f"{cat}-{typ}"
    if key not in part_counters:
        part_counters[key] = 100
    else:
        part_counters[key] += 1
    part_id = part_counters[key]

    df.at[i, 'id'] = f"{cat_id}.{type_id}.{part_id}"

# STEP 9: Save to Excel
try:
    df.to_excel("classified_by_gemini.xlsx", index=False)
    print("\n✅ Done! Saved as classified_by_gemini.xlsx")
except PermissionError:
    print("\n⚠️ Excel file is open. Please close and run again.")
