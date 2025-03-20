import os

# Define product data
product_data = [
    "A123,Product A,10.99,2023-10-15",
    "B456,Product B,15.99,2023-10-15",
    "C789,Product C,20.99,2023-10-15",
    "A123,Product A,9.99,2023-10-16",  # Same product, different price on a different date
    "B456,Product B,14.99,2023-10-16",  # Same product, different price on a different date
]

# Define the output file path (relative to the current folder)
output_file = "products/product_updates.txt"

# Ensure the "products" folder exists
os.makedirs(os.path.dirname(output_file), exist_ok=True)

# Write to the text file in the "products" folder
with open(output_file, mode="w") as file:
    for line in product_data:
        file.write(line + "\n")

print(f"Generated {output_file}")