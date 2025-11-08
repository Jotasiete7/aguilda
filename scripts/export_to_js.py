import pandas as pd
import os
import json

CSV_PATH = "data/processed/trade_data_clean.csv"
JS_PATH = "data/processed/data.js"

def main():
    if not os.path.exists(CSV_PATH):
        print(f"❌ Arquivo não encontrado: {CSV_PATH}")
        return

    try:
        df = pd.read_csv(CSV_PATH)

        if df.empty:
            print("⚠️ CSV está vazio. Nada será exportado.")
            return

        # Substituir NaN e None por string vazia
        df = df.fillna("")

        # Converter para lista de listas (cada linha = array JS)
        data_list = df.values.tolist()

        # Exportar para arquivo JS compatível com o site
        js_content = "const data = " + json.dumps(data_list, ensure_ascii=False, indent=2) + ";"
        os.makedirs(os.path.dirname(JS_PATH), exist_ok=True)
        with open(JS_PATH, "w", encoding="utf-8") as f:
            f.write(js_content)

        print(f"✅ Exportado com sucesso para: {JS_PATH}")
        print(f"💾 Linhas exportadas: {len(data_list)}")

    except Exception as e:
        print(f"❌ Erro ao converter CSV → JS: {e}")

if __name__ == "__main__":
    main()
