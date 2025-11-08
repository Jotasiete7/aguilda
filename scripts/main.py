import os
import re
import pandas as pd
from glob import glob

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"
CSV_OUT = os.path.join(PROCESSED_DIR, "trade_data_clean.csv")
JS_OUT = os.path.join(PROCESSED_DIR, "data.js")

# ============================
# 1. FUNÇÕES AUXILIARES
# ============================

def parse_value(text):
    """Converte 1s50c → 150c / 1.5s → 150c / 120c → 120c (retorna em cobre)"""
    text = str(text).lower().strip()
    silver = re.findall(r"(\d+(?:\.\d+)?)s", text)
    copper = re.findall(r"(\d+(?:\.\d+)?)c", text)
    total = 0
    if silver:
        total += float(silver[0]) * 100
    if copper:
        total += float(copper[0])
    if total == 0 and text.isdigit():
        total = float(text)
    return round(total, 2) if total > 0 else None

def categorize_item(item):
    item = item.lower()
    if any(k in item for k in ["adamantine", "adam lump"]):
        return "Adamantine"
    if any(k in item for k in ["glimmersteel", "glimmer lump"]):
        return "Glimmersteel"
    if any(k in item for k in ["seryll"]):
        return "Seryll"
    if any(k in item for k in ["rare", "supreme", "fantastic"]):
        return "Raros"
    if any(k in item for k in ["hammer", "sword", "axe", "bow"]):
        return "Armas"
    if any(k in item for k in ["horse", "wagon", "cart", "ship"]):
        return "Transportes"
    if any(k in item for k in ["armor", "chain", "plate", "helmet"]):
        return "Armaduras"
    if any(k in item for k in ["dye", "paint"]):
        return "Tintas"
    if any(k in item for k in ["rune", "fragment", "shard"]):
        return "Runas / Fragmentos"
    return "Outros"

def confidence_level(count):
    if count >= 30:
        return "Alta"
    elif count >= 10:
        return "Média"
    return "Baixa"

# ============================
# 2. PROCESSAMENTO PRINCIPAL
# ============================

def main():
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    trade_files = sorted(glob(os.path.join(RAW_DIR, "Trade.2025-*.txt")))
    if not trade_files:
        print("❌ Nenhum arquivo Trade.2025-XX.txt encontrado em data/raw/")
        return

    print(f"📦 Arquivos detectados: {len(trade_files)}")

    registros = []
    for file in trade_files:
        print(f"→ Lendo {os.path.basename(file)}...")
        with open(file, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                parts = [p.strip() for p in line.split("|")]
                if len(parts) < 4:
                    continue
                timestamp, tipo, player, item = parts[:4]
                if tipo.upper() not in ["WTB", "WTS", "PC"]:
                    continue
                registros.append({
                    "timestamp": timestamp.strip(),
                    "type": tipo.upper(),
                    "player": player.strip(),
                    "item": item.strip().lower()
                })

    df = pd.DataFrame(registros)
    print(f"✅ Linhas importadas: {len(df)}")

    # Limpeza
    df["item"] = df["item"].str.replace(r"[^a-zA-Z0-9\s'/-]", " ", regex=True).str.strip()
    df = df[df["item"].str.len() > 2]

    # Categorização
    df["categoria"] = df["item"].apply(categorize_item)

    # Simulação de preço baseado em frequência (placeholder se não houver dados de preço)
    freq = df["item"].value_counts().to_dict()
    df["preco_2024"] = df["item"].apply(lambda x: parse_value(freq.get(x, 100)))
    df["preco_2025"] = df["preco_2024"] * 1.05  # leve aumento simulado
    df["variacao"] = ((df["preco_2025"] - df["preco_2024"]) / df["preco_2024"] * 100).round(1)
    df["indicador"] = df["variacao"].apply(lambda x: "▲" if x > 0 else ("▼" if x < 0 else "—"))
    df["confianca"] = df["item"].map(lambda x: confidence_level(freq.get(x, 1)))
    df["venda"] = df["preco_2025"]

    # Salvar CSV
    df_export = df[["item", "categoria", "preco_2024", "preco_2025",
                    "variacao", "indicador", "confianca", "venda"]]
    df_export.to_csv(CSV_OUT, index=False)
    print(f"💾 Arquivo CSV salvo: {CSV_OUT} ({len(df_export)} linhas)")

    # ============================
    # 3. GERAR data.js PARA O SITE
    # ============================
    print("🧩 Gerando data.js para o site...")
    js_array = ",\n      ".join([
        f'["{r.item}","{r.categoria}",{r.preco_2024},{r.preco_2025},{r.variacao},"{r.indicador}","{r.confianca}",{r.venda}]'
        for r in df_export.itertuples(index=False)
    ])
    with open(JS_OUT, "w", encoding="utf-8") as f:
        f.write(f"const data = [\n      {js_array}\n];")
    print(f"✅ data.js gerado com sucesso: {JS_OUT}")

    # ============================
    # 4. LOG DE INSIGHTS
    # ============================
    print("\n📊 Top 10 Itens Mais Frequentes:")
    print(df["item"].value_counts().head(10))

    print("\n💰 Top 10 Itens com Maior Preço:")
    print(df_export.sort_values("preco_2025", ascending=False).head(10)[["item", "preco_2025", "categoria", "confianca"]])

    print("\n✅ Processo concluído com sucesso.")

if __name__ == "__main__":
    main()
