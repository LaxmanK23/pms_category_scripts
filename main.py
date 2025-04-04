import pandas as pd
import google.generativeai as genai
import time

genai.configure(api_key="AIzaSyDksJ0SAZa_fiPOZOWeUb7Ejc-TnlNMRY4")

df = pd.read_excel("myexcel.xlsx").fillna("")
def make_prompt(row):
    return f"""
You are a classification model for ship part inventory.

Each part should be labeled with:
- **type**: component, spare, or store
- **category**: choose *one only* from the list below, based on the function and location of the part.

CATEGORIES:
1. Ship General – Universal ship elements
2. Hull – Plates, structure, welds
3. Equipment For Cargo – Cargo valves, pipelines
4. Ship Equipment – Anchor, winch, mooring
5. Equipment For Crew And Passengers – Beds, ladders, toilets
6. Machinery Main Components – Pistons, crank, cylinder, compressor
7. Systems For Machinery Main Components – Pumps, coolers, lubrication
8. Ship Common Systems – Fire, bilge, ballast
9. HVAC System – Air conditioning, blowers, ventilation

EXAMPLES:
- “PISTON RING, FC250” → component, Machinery Main Components  
- “OIL RING, FC250” → spare, Machinery Main Components  
- “ACCOMM LADDER” → spare, Equipment For Crew And Passengers  
- “A/C UNIT, HI-AIR” → component, HVAC System  

Now classify this part:

Component Name: {row['component name']}
Drawing Info: {row['Drawing Info']}
Part Name: {row['Part Name']}
Equipment: {row['equipment']}

Answer format:
type: <component | spare | store>
category: <choose from list above>
"""
model = genai.GenerativeModel("gemini-2.5-pro-exp-03-25")
df['type'] = ''
df['category'] = ''
for i, row in df.head(5).iterrows():
    prompt = make_prompt(row)

    try:
        response = model.generate_content(prompt)
        output = response.text.strip()

        for line in output.split('\n'):
            if "type:" in line.lower():
                df.at[i, 'type'] = line.split(":")[-1].strip().lower()
            elif "category:" in line.lower():
                df.at[i, 'category'] = line.split(":")[-1].strip()
    except Exception as e:
        print(f"Error on row {i}: {e}")
        df.at[i, 'type'] = 'error'
        df.at[i, 'category'] = 'error'
    print(f"Processed row {i + 1}")
    time.sleep(1) 
df.to_excel("classified_by_gemini.xlsx", index=False)
print("✅ Done! Saved as classified_by_gemini.xlsx")
