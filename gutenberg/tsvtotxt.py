import pandas as pd
import os
from tqdm import tqdm

def save_texts_to_files():
    # Load data from the Excel file.
    df = pd.read_excel('gutenberg_books.tsv')

    # Create the 'texts' directory if it doesn't exist.
    if not os.path.exists('texts'):
        os.makedirs('texts')

    # Save the text of each book into a separate file.
    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Saving books"):
        try:
            # Generate file name by removing special characters and limiting the length.
            title = ''.join([c for c in row['Title'] if c.isalnum() or c in " _.,"])[:50]
            file_path = os.path.join('texts', f"{row['ID']}_{title}.txt")
            with open(file_path, 'w', encoding='utf-8') as file:
                file.write(row['Text'])
            print(f"Saved {file_path}")
        except Exception as e:
            print(f"Failed to save {file_path}: {e}")

if __name__ == "__main__":
    save_texts_to_files()
