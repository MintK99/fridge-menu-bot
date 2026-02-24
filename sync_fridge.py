import os
import json
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI

# OpenAI
client = OpenAI()

# Google credentials
google_creds_json = os.environ.get("GOOGLE_CREDENTIALS")

creds_dict = json.loads(google_creds_json)

scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=scopes
)

gc = gspread.authorize(creds)

sheet = gc.open_by_key(os.environ["SHEET_ID"]).sheet1
rows = sheet.get_all_values()

inventory = rows[1:]

response = client.chat.completions.create(
    model="gpt-4o-mini",
    response_format={"type": "json_object"},
    temperature=0.5,
    messages=[
        {"role": "system", "content": "Return JSON with 3 dinner menu ideas."},
        {"role": "user", "content": f"Inventory: {inventory}"}
    ]
)

print(response.choices[0].message.content)
