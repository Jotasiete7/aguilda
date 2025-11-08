import pandas as pd
import os
import json

# Caminhos
CSV_FILE = "data/processed/trade_data_clean.csv"
JS_FILE = "data/processed/data.js"

def main():
    print("📦 Convertendo CSV para JS...")

    # Verifica se o arquivo existe
    if not os.path.exists(CSV_FILE):
        print(f"❌ ERRO: Arquivo não encontrado: {CSV_FILE}")
        return

    try:
        # Carrega CSV
        df = pd.read_csv(CSV_FILE)

        # Substitui valores NaN e converte pra tipos básicos
        df = df.fillna("").astype(str)

        # Converte para lista de listas (para o JS)
        data_list = df.values.tolist()

        # Cria conteúdo JS
        js_content = "const data = " + json.dumps(data_list, ensure_ascii=False, indent=2) + ";"

        # Garante que o diretório existe
        os.makedirs(os.path.dirname(JS_FILE), exist_ok=True)

        # Escreve o arquivo final
        with open(JS_FILE, "w", encoding="utf-8") as f:
            f.write(js_content)

        print(f"✅ Arquivo gerado com sucesso: {JS_FILE}")

    except Exception as e:
        print(f"❌ Erro ao converter CSV para JS: {e}")

if __name__ == "__main__":
    main()
