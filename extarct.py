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
You are a classification model for ship pms components.
you will be given a excel file with a list of ship pms components.
 like: 
plan code,	vessel plan code,	componet plan code,	plan name,	component,	manufacture,	value,	frequency,	responsibility,	parent component														
ZHEOS01,	   HTR001,	    HTRJB001,	FIVE YEAR ROUTINE - HEATER,	HEATER COMPLETE,	OSAKA STEAM HEATER STANEX S154,	60,	M,	Engineer, heater	
VHFFUFM2,	VHF001,	VHFJB001,	CHECK OPERATION,	COMPLETE 2-WAY VHF,	2-WAY VHF RADIO TELEPHONE FURUNO FM-8,	1,	M,	D, vhf system																
VHFFUFM2,	VHF001,	VHFJB002,	CHECK/CHANGE BATTERY	,COMPLETE 2-WAY VHF	,2-WAY VHF RADIO TELEPHONE FURUNO FM-8	,12,	M,	D, vhf system																
VHFFUFM2,	VHF001,	VHFJB003,	CHECK GEN. CONDITION,	COMPLETE 2-WAY VHF,	2-WAY VHF RADIO TELEPHONE FURUNO FM-8,	1,	M,	D, vhf system																


now you will first arrange them according their real word hierarchy.
then first add one column "category" and classify them into one of the following categories:
01 Hull Integrity
02 Manoeuvrability
03 Mooring
04 Cargo Handling
05 Communication
06 Navigation
07 Other

then give them code like this:
for first give code 01.001  here 01 is categoy then first hierarchy componet
for second give code 01.002 here 01 is categoy then second

now levl 2 give them code like this:
01.001.001  here 01 is categoy then first hierarchy componet then first subcomponent
for second give code 01.001.002 here 01 is categoy then first hierarchy componet then second subcomponent
and so on 
it will be a new column "code" in the excel file.
the output will be a new excel file with the same columns and two new columns "category" and "code".

"""
    return prompt




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
