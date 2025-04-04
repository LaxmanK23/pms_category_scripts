import os
import pandas as pd
import google.generativeai as genai
import concurrent.futures
import time
import getpass

# === SETTINGS ===
SOURCE_FILE = "myexcel.xlsx"
CHUNK_FOLDER = "chunks"
OUTPUT_FOLDER = "outputs"
CHUNK_SIZE = 5000
API_MODEL = "gemini-2.0-flash"
THREADS = 3
BATCH_SIZE = 100

# === SETUP ===
os.makedirs(CHUNK_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    print("🔑 Gemini API key not found in environment.")
    api_key = getpass.getpass("Please enter your Gemini API key (input hidden): ")
genai.configure(api_key=api_key)
model = genai.GenerativeModel(API_MODEL)

category_id_map = {
    "Ship General": 1, "Hull": 2, "Equipment For Cargo": 3, "Ship Equipment": 4,
    "Equipment For Crew And Passengers": 5, "Machinery Main Components": 6,
    "Systems For Machinery Main Components": 7, "Ship Common Systems": 8, "HVAC System": 9
}

# === STEP 1: SPLIT EXCEL ONCE ===
def split_excel_to_chunks():
    df = pd.read_excel(SOURCE_FILE).fillna("")
    for i in range(0, len(df), CHUNK_SIZE):
        chunk = df.iloc[i:i + CHUNK_SIZE]
        chunk.reset_index(drop=True, inplace=True)
        chunk_path = os.path.join(CHUNK_FOLDER, f"chunk_{i // CHUNK_SIZE + 1}.xlsx")
        if not os.path.exists(chunk_path):
            chunk.to_excel(chunk_path, index=False)
            print(f"📦 Created: {chunk_path}")

# === PROMPT BUILDER ===
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

# === CLASSIFICATION FUNCTION ===
def classify_chunk(start_idx, rows):
    results = []
    try:
        prompt = make_prompt(rows)
        response = model.generate_content(prompt)
        output = response.text.strip()
        parts = output.split("Part ")[1:]

        for i, part_output in enumerate(parts):
            type_val, category_val = "error", "error"
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
    time.sleep(1)
    return results

# === PROCESS CHUNK FILE ===
def process_excel_chunk(chunk_path, output_path):
    df = pd.read_excel(chunk_path).fillna("")
    df['type'], df['category'], df['id'] = '', '', ''

    all_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = []
        for start in range(0, len(df), BATCH_SIZE):
            batch_rows = df.iloc[start:start + BATCH_SIZE].to_dict(orient='records')
            futures.append(executor.submit(classify_chunk, start, batch_rows))
        for future in concurrent.futures.as_completed(futures):
            all_results.extend(future.result())

    for res in all_results:
        df.at[res['index'], 'type'] = res['type']
        df.at[res['index'], 'category'] = res['category']

    # ID assignment
    type_counters = {}
    part_counters = {}
    for i, row in df.iterrows():
        cat, typ = row['category'], row['type']
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

    df.to_excel(output_path, index=False)
    print(f"💾 Saved: {output_path}")

# === MAIN LOOP ===
def main():
    split_excel_to_chunks()
    chunk_files = sorted(f for f in os.listdir(CHUNK_FOLDER) if f.endswith(".xlsx"))
    for chunk_file in chunk_files:
        chunk_path = os.path.join(CHUNK_FOLDER, chunk_file)
        output_path = os.path.join(OUTPUT_FOLDER, f"classified_{chunk_file}")
        if os.path.exists(output_path):
            print(f"⏩ Skipping already processed: {chunk_file}")
            continue
        print(f"🚀 Processing: {chunk_file}")
        process_excel_chunk(chunk_path, output_path)

if __name__ == "__main__":
    main()
