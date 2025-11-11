import pandas as pd
import re
import os
import glob

raw_dir = 'data/raw'
csv_path = 'data/processed/trade_data_clean.csv'
os.makedirs('data/processed', exist_ok=True)

all_data = []
txt_files = glob.glob(os.path.join(raw_dir, '*.txt'))
print(f"🔍 Encontrados {len(txt_files)} arquivos TXT.")

for file in txt_files:
    print(f"📖 Processando {os.path.basename(file)}...")
    try:
        with open(file, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            print(f"   📄 {len(lines)} linhas totais.")
            parsed = 0
            for i, line in enumerate(lines):
                line = line.strip()
                if not line or line.startswith('-'): continue  # Ignora vazias/headers
                
                # 🛠️ REGEX PERFEITA pro seu formato: timestamp | TYPE(qualquer) | user | resto
                match = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) \|\s*(WT[BSPC]|WTA)\s*\|\s*([^|]+?)\s*\|\s*(.*)', line.strip(), re.IGNORECASE)
                if match:
                    timestamp, type_, user, desc = match.groups()
                    type_ = type_.upper().strip()
                    user = user.strip()
                    desc = desc.strip()
                    
                    # 🏷️ Extrai ITEM (primeiras 1-4 palavras) e PREÇO (números + s/c)
                    words = desc.split()
                    item = ' '.join(words[:4]) if words else 'Unknown'  # Ex: "rare null strange bone", "blue dye color"
                    price_match = re.search(r'(\d+(?:\.\d+)?)\s*([sc]?)', ' '.join(words))
                    price = f"{price_match.group(1)}{price_match.group(2)}" if price_match else 'N/A'
                    
                    all_data.append([timestamp, type_, user, item, price, desc])
                    parsed += 1
                    if parsed % 50 == 0: print(f"   ✅ {parsed} parseados...")
                #else: print(f"❌ Falhou: {line[:80]}...")  # Descomente pra debug full
            
            print(f"   🎉 {parsed} linhas OK de {len(lines)}!")
    except Exception as e:
        print(f"   ❌ Erro no arquivo: {e}")

df = pd.DataFrame(all_data, columns=['Timestamp', 'Type', 'User', 'Item', 'Price', 'Description'])
print(f"\n📊 TOTAL DATAFRAME: {len(df)} linhas")

if len(df) > 0:
    df.to_csv(csv_path, index=False, encoding='utf-8')
    print(f"💾 CSV salvo: {csv_path} ({len(df)} rows)")
    
    # Preview top 5
    print("🔍 Preview:")
    print(df.head().to_string())
else:
    print("🚨 ALERTA: DataFrame VAZIO! TXT inválido ou regex falhou.")
