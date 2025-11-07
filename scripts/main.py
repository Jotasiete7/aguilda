import pandas as pd
import os
import glob

def clean_trade_data(input_file, output_file):
    try:
        df = pd.read_csv(input_file, sep="\t", engine="python", on_bad_lines="skip")
        df.drop_duplicates(inplace=True)
        if 'value' in df.columns:
            df = df[df['value'] > 0]
        df.to_csv(output_file, index=False)
        print(f"Arquivo limpo salvo em: {output_file}")
    except Exception as e:
        print(f"Erro ao processar {input_file}: {e}")

if __name__ == "__main__":
    input_files = sorted(glob.glob("data/raw/Trade.*.txt"))
    if not input_files:
        print("Nenhum arquivo Trade encontrado em data/raw/")
    else:
        latest_file = input_files[-1]
        output_path = "data/processed/trade_data_clean.csv"
        clean_trade_data(latest_file, output_path)
