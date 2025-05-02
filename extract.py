import os
import pandas as pd
import google.generativeai as genai
import getpass

SOURCE_FILE = "pms_fivestar.xlsx"
CHUNK_FOLDER = "chunks2"
OUTPUT_FOLDER = "outputs2"
CHUNK_SIZE = 5000
API_MODEL = "gemini-2.5-flash-preview-04-17"
THREADS = 3
BATCH_SIZE = 100

os.makedirs(CHUNK_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

api_key = os.getenv("GEMINI_API_KEY")
if not api_key:
    print("🔑 Gemini API key not found in environment.")
    api_key = getpass.getpass("Please enter your Gemini API key (input hidden): ")
genai.configure(api_key=api_key)
model = genai.GenerativeModel(API_MODEL)


def split_excel_to_chunks():
    df = pd.read_excel(SOURCE_FILE).fillna("")
    for i in range(0, len(df), CHUNK_SIZE):
        chunk = df.iloc[i:i + CHUNK_SIZE]
        chunk.reset_index(drop=True, inplace=True)
        chunk_path = os.path.join(CHUNK_FOLDER, f"chunk_{i // CHUNK_SIZE + 1}.xlsx")
        if not os.path.exists(chunk_path):
            chunk.to_excel(chunk_path, index=False)
            print(f"📦 Created: {chunk_path}")


def make_prompt(rows):
    prompt = """
    You are a classification model for ship PMS (Planned Maintenance System) components. You will be given a table with a list of ship PMS components in the following format:

plan code, vessel plan code, component plan code, plan name, component, manufacturer, value, frequency, responsibility, parent component.

Example entries:
- ZHEOS01, HTR001, HTRJB001, FIVE YEAR ROUTINE - HEATER, HEATER COMPLETE, OSAKA STEAM HEATER STANEX S154, 60, M, Engineer, heater
- VHFFUFM2, VHF001, VHFJB001, CHECK OPERATION, COMPLETE 2-WAY VHF, 2-WAY VHF RADIO TELEPHONE FURUNO FM-8, 1, M, D, vhf system
- VHFFUFM2, VHF001, VHFJB002, CHECK/CHANGE BATTERY, COMPLETE 2-WAY VHF, 2-WAY VHF RADIO TELEPHONE FURUNO FM-8, 12, M, D, vhf system
- VHFFUFM2, VHF001, VHFJB003, CHECK GEN. CONDITION, COMPLETE 2-WAY VHF, 2-WAY VHF RADIO TELEPHONE FURUNO FM-8, 1, M, D, vhf system

### Instructions:
1. **Hierarchy and Classification:**
   - Arrange the components in a logical hierarchy based on their relationship to one another. For instance, subcomponents (like "COMPLETE 2-WAY VHF") should be grouped under their parent component (like "VHF system").
   - Assign a **category** to each component based on the following predefined categories:
     - `01 Hull Integrity`
     - `02 Manoeuvrability`
     - `03 Mooring`
     - `04 Cargo Handling`
     - `05 Communication`
     - `06 Navigation`
     - `07 Other`

2. **Category and Code Generation:**
   - Add a new column labeled **"category"** where you classify each component into one of the above categories.
   - Generate a unique **"code"** for each component based on its category and hierarchy:
     - The **code** format should be as follows:
       - **Level 1** (Primary component): `XX.001` (e.g., `01.001` for the first item under "Hull Integrity").
       - **Level 2** (Subcomponents): `XX.001.001`, `XX.001.002`, etc.
       - The number after the period should represent the position in the hierarchy.
   
3. **Output Requirements:**
   - You need to provide an updated table with the same columns as the input, plus two new columns: **"category"** and **"code"**.
   - Ensure that each row's "category" and "code" reflect its position and type within the hierarchy.
   
4. **Example Output:**
   After processing, your table should look something like this:

| plan code | vessel plan code | component plan code | plan name         | component            | manufacturer              | value | frequency | responsibility | parent component | category  | code     |
|-----------|------------------|---------------------|-------------------|-----------------------|---------------------------|-------|-----------|----------------|------------------|-----------|----------|
| ZHEOS01   | HTR001           | HTRJB001             | FIVE YEAR ROUTINE - HEATER | HEATER COMPLETE      | OSAKA STEAM HEATER STANEX S154 | 60    | M         | Engineer        | heater           | 01 Hull Integrity | 01.001  |
| VHFFUFM2  | VHF001           | VHFJB001             | CHECK OPERATION   | COMPLETE 2-WAY VHF     | 2-WAY VHF RADIO TELEPHONE FURUNO FM-8 | 1     | M         | D              | vhf system       | 05 Communication   | 05.001  |
| VHFFUFM2  | VHF001           | VHFJB002             | CHECK/CHANGE BATTERY | COMPLETE 2-WAY VHF   | 2-WAY VHF RADIO TELEPHONE FURUNO FM-8 | 12    | M         | D              | vhf system       | 05 Communication   | 05.002  |
| VHFFUFM2  | VHF001           | VHFJB003             | CHECK GEN. CONDITION | COMPLETE 2-WAY VHF  | 2-WAY VHF RADIO TELEPHONE FURUNO FM-8 | 1     | M         | D              | vhf system       | 05 Communication   | 05.003  |

### Additional Notes:
- If the component doesn't clearly fall into one of the predefined categories, classify it as "07 Other".
- Ensure that the hierarchy is logical and follows a natural structure based on the data's relationships (e.g., subcomponents under their parent components).

"""
    return prompt


def process_excel_chunk(chunk_path, output_path):
    df = pd.read_excel(chunk_path).fillna("")
    
    prompt = make_prompt(df.to_dict(orient="records"))

    # Generate content using the model
    response = model.generate_content(prompt)
    print(f"🔍 Processing chunk: {response}")
    
    # Extract the content from the response (access the first part of the response)
    if response.candidates and len(response.candidates) > 0:
        # Accessing 'text' from the first part of the response
        content = response.candidates[0].content
        classified_data = content.parts[0].text if content.parts else None
        
        if classified_data:
            # Optionally, print out the classified data to ensure it's correct
            print(f"Classified data:\n{classified_data}")

            categories = []
            codes = []
            
            # Now, parse the response content and classify each component accordingly
            for index, row in df.iterrows():
                category = "01"  # Placeholder: Replace with actual category parsing from `classified_data`
                code = f"{category}.{index + 1:03d}"  # Placeholder: Replace with actual code generation
                categories.append(category)
                codes.append(code)
            
            # Add the category and code columns to the DataFrame
            df["category"] = categories
            df["code"] = codes
            
            # Save the updated DataFrame to an output Excel file
            df.to_excel(output_path, index=False)
            print(f"✅ Processed and saved: {output_path}")
        else:
            print(f"⚠️ No classified data available in the response.")
    else:
        print(f"⚠️ Error processing {chunk_path}: No valid response from AI.")



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
