import os
import pandas as pd
import re

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
CSV_FILE = os.path.join(PROCESSED_DIR, "trade_data_clean.csv")
JS_FILE = os.path.join(PROCESSED_DIR, "data.js")

def get_latest_trade_file():
    trade_files = [f for f in os.listdir(RAW_DIR) if re.match(r"Trade\.\d{4}-\d{2}\.txt$", f)]
    if not trade_files:
        raise FileNotFoundError("Nenhum arquivo Trade.YYYY-MM.txt encontrado em data/raw/")
    trade_files.sort(reverse=True)
    return os.path.join(RAW_DIR, trade_files[0])

def parse_trade_lines(filepath):
    pattern_sell = re.compile(r"sold\s+(.*?)\s+for\s+([\d\.]+)([sc])", re.IGNORECASE)
    pattern_buy = re.compile(r"bought\s+(.*?)\s+for\s+([\d\.]+)([sc])", re.IGNORECASE)
    rows = []

    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            m = pattern_sell.search(line)
            if m:
                item, value, unit = m.groups()
                price = float(value) * (100 if unit == "c" else 10000)
                rows.append([item.strip(), "Venda", price])
                continue
            m = pattern_buy.search(line)
            if m:
                item, value, unit = m.groups()
                price = float(value) * (100 if unit == "c" else 10000)
                rows.append([item.strip(), "Compra", price])

    df = pd.DataFrame(rows, columns=["item", "categoria", "preco_2025"])
    return df

def clean_trade_data(df):
    df["preco_2024"] = df["preco_2025"] * 0.95  # valor fictício p/ referência
    df["variacao"] = ((df["preco_2025"] - df["preco_2024"]) / df["preco_2024"] * 100).round(1)
    df["indicador"] = df["variacao"].apply(lambda v: "▲" if v > 0 else "▼" if v < 0 else "—")
    df["confianca"] = "Média"
    df["venda"] = df["preco_2025"]
    return df[["item", "categoria", "preco_2024", "preco_2025", "variacao", "indicador", "confianca", "venda"]]

def save_csv(df):
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    df.to_csv(CSV_FILE, index=False)
    print(f"✅ CSV salvo em: {CSV_FILE} ({len(df)} linhas)")

def save_js(df):
    js_content = "const data = [\n"
    for _, row in df.iterrows():
        js_row = [str(x).replace("'", "\\'") for x in row.tolist()]
        js_content += f"  {js_row},\n"
    js_content += "];\n"
    with open(JS_FILE, "w", encoding="utf-8") as f:
        f.write(js_content)
    print(f"✅ JS salvo em: {JS_FILE}")

def main():
    trade_file = get_latest_trade_file()
    print(f"📦 Lendo arquivo: {trade_file}")
    df = parse_trade_lines(trade_file)
    if df.empty:
        print("⚠️ Nenhum registro encontrado no arquivo!")
        return
    clean_df = clean_trade_data(df)
    save_csv(clean_df)
    save_js(clean_df)

if __name__ == "__main__":
    main()
