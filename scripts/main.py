import os
import re
import pandas as pd
from glob import glob

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
OUTPUT_FILE = os.path.join(PROCESSED_DIR, "trade_data_clean.csv")

# === 1. Encontrar arquivos válidos ===
def get_trade_files():
    files = sorted(glob(os.path.join(RAW_DIR, "Trade.*.txt")))
    if not files:
        raise FileNotFoundError("Nenhum arquivo Trade.YYYY-MM.txt encontrado em data/raw/")
    print(f"📦 Arquivos detectados: {len(files)}")
    return files

# === 2. Detectar se é tabulado ou cru ===
def is_tabulated(file_path):
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for _ in range(10):
            line = f.readline()
            if "," in line or "\t" in line:
                return True
    return False

# === 3. Parser para arquivos tabulados ===
def parse_tabulated(file_path):
    print(f"→ Lendo tabulado: {os.path.basename(file_path)}")
    df = pd.read_csv(file_path, sep=None, engine="python", on_bad_lines="skip")
    if "item" not in df.columns:
        df.columns = [c.strip().lower() for c in df.columns]
    print(f"✅ Linhas importadas: {len(df)}")
    return df

# === 4. Parser para logs crus ===
def parse_raw(file_path):
    print(f"→ Lendo bruto: {os.path.basename(file_path)}")
    pattern = re.compile(r"(\d{4}-\d{2}-\d{2}.*?)\s*\|\s*(WTB|WTS|PC)\s*\|\s*([^|]+)\s*\|\s*(.*)", re.IGNORECASE)
    records = []
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            m = pattern.match(line)
            if not m:
                continue
            timestamp, tipo, player, item = m.groups()
            item = clean_item(item)
            if item:
                records.append([timestamp, tipo.upper(), player.strip(), item])
    df = pd.DataFrame(records, columns=["timestamp", "type", "player", "item"])
    print(f"✅ Linhas importadas: {len(df)}")
    return df

# === 5. Limpeza de item (herdado e aprimorado) ===
def clean_item(text):
    s = text
    s = re.sub(r"\[.*?\]|\(.*?\)", "", s)
    s = re.sub(r"\b\d+k\b|\b\d+(\.\d+)?(s|c|kg|ql)\b", "", s, flags=re.IGNORECASE)
    s = re.sub(r"http\S+", "", s)
    s = re.sub(r"[^a-zA-Z0-9\s'/-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s if len(s) > 1 else None

# === 6. Normalização e agrupamento ===
def unify_dataframes(dfs):
    df = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=["type", "player", "item"])
    df["item"] = df["item"].astype(str)
    df["categoria"] = df["item"].apply(classify_category)
    df["preco_2024"] = df.groupby("item").ngroup() * 10 + 100  # mock para visualização
    df["preco_2025"] = df["preco_2024"] * (1 + (pd.Series(range(len(df))) % 5) / 100)
    df["variacao"] = ((df["preco_2025"] - df["preco_2024"]) / df["preco_2024"] * 100).round(1)
    df["indicador"] = df["variacao"].apply(lambda v: "▲" if v > 0 else "▼" if v < 0 else "—")
    df["confianca"] = "Média"
    df["venda"] = df["preco_2025"] * 1.1
    df = df[["item", "categoria", "preco_2024", "preco_2025", "variacao", "indicador", "confianca", "venda"]]
    return df

# === 7. Classificação simples de categoria ===
def classify_category(item):
    if any(x in item for x in ["brick", "plank", "beam", "shard", "clay", "mortar", "log"]):
        return "Bulk"
    if any(x in item for x in ["lump", "ore", "metal", "steel", "iron", "silver", "gold"]):
        return "Metais"
    if any(x in item for x in ["chain", "plate", "set", "armour", "helm"]):
        return "Armour"
    if any(x in item for x in ["tool", "imp", "smith", "hammer", "anvil"]):
        return "Blacksmithing"
    if any(x in item for x in ["enchant", "cast", "lt", "coc", "woa"]):
        return "Enchants"
    return "Misc"

# === 8. Salvar CSV processado ===
def save_clean_data(df):
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    df = df.dropna(subset=["item"]).drop_duplicates()
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"💾 Arquivo salvo em: {OUTPUT_FILE} ({len(df)} linhas)")

# === 9. Execução Principal ===
def main():
    trade_files = get_trade_files()
    dfs = []
    for file in trade_files:
        try:
            if is_tabulated(file):
                dfs.append(parse_tabulated(file))
            else:
                dfs.append(parse_raw(file))
        except Exception as e:
            print(f"⚠️ Erro ao processar {file}: {e}")
    if not dfs:
        raise RuntimeError("Nenhum dado processado.")
    df = unify_dataframes(dfs)
    save_clean_data(df)

if __name__ == "__main__":
    main()
