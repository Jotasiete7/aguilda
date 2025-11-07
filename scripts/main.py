import os
import pandas as pd
import re
from pathlib import Path

# === 0. Diretórios base com caminhos absolutos ===
BASE_DIR = Path(__file__).resolve().parent.parent  # raiz do projeto
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
OUTPUT_FILE = PROCESSED_DIR / "trade_data_clean.csv"

# === 1. Localizar o arquivo mais recente no formato Trade.YYYY-MM.txt ===
def get_latest_trade_file():
    trade_files = [
        f for f in os.listdir(RAW_DIR)
        if re.match(r"Trade\.\d{4}-\d{2}\.txt$", f)
    ]
    if not trade_files:
        raise FileNotFoundError("❌ Nenhum arquivo Trade.YYYY-MM.txt encontrado em data/raw/")
    trade_files.sort(reverse=True)
    return RAW_DIR / trade_files[0]

# === 2. Ler arquivo TXT ===
def load_trade_data(filepath):
    try:
        df = pd.read_csv(filepath, sep="\t", engine="python", on_bad_lines="skip")
    except Exception:
        df = pd.read_csv(filepath, sep=None, engine="python", on_bad_lines="skip")
    return df

# === 3. Limpeza e padronização ===
def clean_trade_data(df):
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    possible_cols = {
        "item": ["item", "name", "product"],
        "categoria": ["category", "cat", "tipo"],
        "preco_2024": ["2024", "price_2024", "prev_price"],
        "preco_2025": ["2025", "price_2025", "new_price", "price"],
        "variacao": ["var", "diff", "change", "%"],
        "indicador": ["indicator", "trend", "status"],
        "confianca": ["confidence", "conf", "trust"],
        "venda": ["sell", "sale", "value"]
    }

    col_map = {}
    for key, opts in possible_cols.items():
        for opt in opts:
            match = [c for c in df.columns if opt in c]
            if match:
                col_map[key] = match[0]
                break
    df = df.rename(columns=col_map)

    for key in possible_cols.keys():
        if key not in df.columns:
            df[key] = ""

    for col in ["preco_2024", "preco_2025", "variacao", "venda"]:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", ".")
            .str.extract(r"([\d\.]+)")[0]
            .astype(float, errors="ignore")
        )

    def get_indicator(row):
        try:
            if row["preco_2025"] > row["preco_2024"]:
                return "▲"
            elif row["preco_2025"] < row["preco_2024"]:
                return "▼"
            else:
                return "—"
        except Exception:
            return "—"

    df["indicador"] = df.apply(get_indicator, axis=1)

    if df["variacao"].isnull().all() or (df["variacao"] == "").all():
        df["variacao"] = (
            ((df["preco_2025"] - df["preco_2024"]) / df["preco_2024"] * 100)
            .round(1)
            .fillna(0)
        )

    conf_vals = ["Alta", "Média", "Baixa"]
    df["confianca"] = df["confianca"].replace("", "Média")
    df["confianca"] = df["confianca"].apply(
        lambda x: x if x in conf_vals else "Média"
    )

    df = df[
        ["item", "categoria", "preco_2024", "preco_2025",
         "variacao", "indicador", "confianca", "venda"]
    ]

    return df

# === 4. Salvar CSV processado ===
def save_clean_data(df):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
    print(f"✅ Arquivo salvo em: {OUTPUT_FILE} ({len(df)} linhas)")
    print(f"📁 Local absoluto: {OUTPUT_FILE.resolve()}")

# === 5. Execução principal ===
def main():
    trade_file = get_latest_trade_file()
    print(f"📦 Lendo arquivo: {trade_file}")
    df = load_trade_data(trade_file)
    clean_df = clean_trade_data(df)
    save_clean_data(clean_df)

if __name__ == "__main__":
    main()
