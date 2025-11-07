import os
import pandas as pd
import re

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
CSV_FILE = os.path.join(PROCESSED_DIR, "trade_data_clean.csv")
JS_FILE = os.path.join(PROCESSED_DIR, "data.js")

# === 1. Localizar o arquivo mais recente ===
def get_latest_trade_file():
    trade_files = [
        f for f in os.listdir(RAW_DIR)
        if re.match(r"Trade\.\d{4}-\d{2}\.txt$", f)
    ]
    if not trade_files:
        raise FileNotFoundError("Nenhum arquivo Trade.YYYY-MM.txt encontrado em data/raw/")
    trade_files.sort(reverse=True)
    return os.path.join(RAW_DIR, trade_files[0])

# === 2. Ler arquivo TXT ===
def load_trade_data(filepath):
    try:
        df = pd.read_csv(filepath, sep="\t", engine="python", on_bad_lines="skip")
    except Exception:
        df = pd.read_csv(filepath, sep=None, engine="python", on_bad_lines="skip")
    return df

# === 3. Limpeza ===
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
        except:
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

    df = df[[
        "item", "categoria", "preco_2024", "preco_2025",
        "variacao", "indicador", "confianca", "venda"
    ]]

    return df

# === 4. Salvar CSV ===
def save_csv(df):
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    df.to_csv(CSV_FILE, index=False)
    print(f"✅ CSV salvo em: {CSV_FILE} ({len(df)} linhas)")

# === 5. Salvar JS ===
def save_js(df):
    js_content = "const data = [\n"
    for _, row in df.iterrows():
        js_row = [str(x).replace("'", "\\'") for x in row.tolist()]
        js_content += f"  {js_row},\n"
    js_content += "];\n"
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    with open(JS_FILE, "w", encoding="utf-8") as f:
        f.write(js_content)
    print(f"✅ JS salvo em: {JS_FILE}")

# === 6. Execução principal ===
def main():
    trade_file = get_latest_trade_file()
    print(f"📦 Lendo arquivo: {trade_file}")
    df = load_trade_data(trade_file)
    clean_df = clean_trade_data(df)
    save_csv(clean_df)
    save_js(clean_df)

if __name__ == "__main__":
    main()
