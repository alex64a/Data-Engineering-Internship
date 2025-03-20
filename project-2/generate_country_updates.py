import os

# Define country data with correct CountryID format
country_data = [
    "1101,Germany",
    "1102,Italy",
    "1002,France",
    "1003,United Kingdom",
    "1003,Spain",
    "2201,Nigeria",
    "2202,Egypt",
    "2203,South Africa",
    "3301,China",
    "3302,India",
    "3303,Japan",
    "4401,Australia",
    "4402,New Zealand",
    "5501,United States",
    "5502,Canada",
    "5503,Mexico",
    "6601,Brazil",
    "6602,Argentina",
    "6603,Chile",
    "1301,""Russia",
    "1302,Turkey",
]
# Define the output file path (relative to the current folder)
output_file = "countries/country_updates.txt"

# Ensure the "countries" folder exists
os.makedirs(os.path.dirname(output_file), exist_ok=True)

# Write to the text file in the "countries" folder
with open(output_file, mode="w") as file:
    for line in country_data:
        file.write(line + "\n")

print(f"Generated {output_file}")