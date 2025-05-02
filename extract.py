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


def make_prompt(data):
    """
    This function generates a prompt for the AI model to categorize the ship PMS components.
    The categories are predefined, and the model should classify each row accordingly.
    """

    prompt = "Please classify the following ship PMS components based on their type and functionality into one of the predefined categories. Then, assign each a unique code.\n\n"
    for row in data:
        # Use the correct column name for manufacture
        plan_name = row.get('plan name', 'N/A')
        component = row.get('component', 'N/A')
        manufacture = row.get('manufacture', 'N/A')  # Corrected column name
        value = row.get('value', 'N/A')

        prompt += f"Plan Name: {plan_name}, Component: {component}, Manufacturer: {manufacture}, Value: {value}\n"

    prompt += "\nCategories to classify components into:\n"
    prompt += "01 Hull Integrity\n02 Auxiliary Engine\n03 Electrical System\n04 Safety Equipment\n05 Communication\n06 Navigation\n"
    prompt += "Generate the category and code for each task in the format `XX.YYY`.\n"
    return prompt



def process_excel_chunk(chunk_path, output_path):
    df = pd.read_excel(chunk_path).fillna("")
    
    # Generate the prompt with the data
    prompt = make_prompt(df.to_dict(orient="records"))

    # Generate content using the model
    response = model.generate_content(prompt)
    
    # Extract the content from the response (access the first part of the response)
    if response.candidates and len(response.candidates) > 0:
        content = response.candidates[0].content
        classified_data = content.parts[0].text if content.parts else None
        
        if classified_data:
            # Print the classified data (optional, for debugging)
            print(f"Classified data:\n{classified_data}")

            # Here, categorize and generate codes based on the model response
            categories = []
            codes = []
            
            for index, row in df.iterrows():
                # Implement logic to map the category based on the model response
                if 'Auxiliary Engine' in classified_data:  # Use the model's response to assign categories
                    category = "02"  # For Auxiliary Engine
                else:
                    category = "01"  # Default to Hull Integrity (you can add more conditions based on other categories)

                # Generate a unique code for each entry
                code = f"{category}.{index + 1:03d}"
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
        # if os.path.exists(output_path):
        #     print(f"⏩ Skipping already processed: {chunk_file}")
        #     continue
        print(f"🚀 Processing: {chunk_file}")
        process_excel_chunk(chunk_path, output_path)


if __name__ == "__main__":
    main()
