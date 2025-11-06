import pandas as pd
import os

def clean_trade_data(input_file, output_file):
    df = pd.read_csv(input_file)
    df.drop_duplicates(inplace=True)
    df = df[df['value'] > 0]
    df.to_csv(output_file, index=False)
    print(f"Arquivo limpo salvo em: {output_file}")

if __name__ == "__main__":
    input_path = "data/raw/trade_data.csv"
    output_path = "data/processed/trade_data_clean.csv"
    if os.path.exists(input_path):
        clean_trade_data(input_path, output_path)
    else:
        print("Arquivo de entrada não encontrado.")
