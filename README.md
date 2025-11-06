# Wurm Trade Engine

Este repositório contém o sistema automatizado de análise de comércio do Wurm Online.

## Estrutura

- **scripts/**: scripts Python para processar e limpar dados
- **data/raw/**: arquivos originais (txt, csv)
- **data/processed/**: dados tratados
- **data/insights/**: análises e resultados
- **.github/workflows/**: automações via GitHub Actions

## Execução local

```bash
python scripts/main.py
```

## Automação

O GitHub Actions detecta novos arquivos em `data/raw/` e executa o script automaticamente.
