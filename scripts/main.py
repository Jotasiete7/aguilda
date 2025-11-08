import os
import re
import pandas as pd
from glob import glob

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
OUTPUT_FILE = os.path.join(PROCESSED_DIR, "trade_data_clean.csv")

# Regex para o formato de linha
pattern = re.compile(
    r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\s*\|\s*(WTB|WTS|PC)\s*\|\s*([\w\d]+)\s*\|\s*(.*)",
    re.IGNORECASE
)

NOISE_WORDS = [
    'wtb','wts','pc','wtt','want','buy','sell','price','check','cod','ea','each',
    'per','for','tinyurl','forum','wurmonline','pm','dm','contact','http','https'
]

def get_latest_trade_file():
    files = sorted(
        [f for f in os.listdir(RAW_DIR) if re.match(r"(Trade\.\d{4}-\d{2}\.txt|wurm_trade_master_\d{4}_clean\.txt)", f)],
        reverse=True
    )
    if not files:
        raise FileNotFoundError("Nenhum arquivo Trade.* encontrado em data/raw/")
    return os.path.join(RAW_DIR, files[0])

def clean_item(text):
    s = re.sub(r"\[.*?\]", "", text)
    s = re.sub(r"ql[:=]?\s*\d+(\.\d+)?", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\b\d+k\b|\b\d+kg\b|\b\d+g\b|\b\d+(\.\d+)?s\b|\b\d+(\.\d+)?c\b|\b\d+\b", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"http\S+", "", s)
    s = re.sub(r"[^a-zA-Z0-9\s'\/-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    for noise in NOISE_WORDS:
        s = re.sub(rf"\b{re.escape(noise)}\b", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip()
    return s if s else None

def parse_trade_files():
    records = []
    files = sorted(glob(os.path.join(RAW_DIR, "Trade.2025-*.txt")))
    if not files:
        print("⚠️ Nenhum arquivo Trade.2025-XX.txt encontrado.")
        return pd.DataFrame()

    for file in files:
        print(f"> Lendo {file} ...")
        with open(file, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                m = pattern.match(line)
                if not m:
                    continue
                tipo, player, rest = m.groups()
                if player.lower() == "system":
                    continue
                tipo = tipo.upper()
                items = re.split(r",|\band\b|\bor\b|/|;|-|\||\\n", rest, flags=re.IGNORECASE)
                for it in items:
                    item_clean = clean_item(it)
                    if item_clean:
                        records.append([tipo, player, item_clean])

    df = pd.DataFrame(records, columns=["tipo", "player", "item"])
    df = df.dropna(subset=["item"])
    df = df.drop_duplicates(subset=["tipo", "player", "item"])
    return df

def summarize_data(df):
    if df.empty:
        print("⚠️ Nenhum dado processado.")
        return pd.DataFrame(columns=[
            "item","categoria","preco_2024","preco_2025","variacao","indicador","confianca","venda"
        ])

    item_counts = df.groupby(["item","tipo"]).size().reset_index(name="count")
    item_counts["preco_2025"] = item_counts["count"] * 100
    item_counts["preco_2024"] = item_counts["preco_2025"] * 0.95
    item_counts["variacao"] = ((item_counts["preco_2025"] - item_counts["preco_2024"]) / item_counts["preco_2024"] * 100).round(1)
    item_counts["indicador"] = "▲"
    item_counts["confianca"] = "Média"
    item_counts["venda"] = item_counts["preco_2025"]
    item_counts["categoria"] = item_counts["tipo"].replace({
        "WTS":"Venda","WTB":"Compra","PC":"Referência"
    })
    return item_counts[["item","categoria","preco_2024","preco_2025","variacao","indicador","confianca","venda"]]

def main():
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    trade_file = get_latest_trade_file()
    print(f"📦 Arquivo: {trade_file}")
    df = parse_trade_files()
    print(f"✅ Linhas importadas: {len(df)}")
    clean_df = summarize_data(df)
    clean_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Arquivo salvo: {OUTPUT_FILE} ({len(clean_df)} linhas)")

if __name__ == "__main__":
    main()
