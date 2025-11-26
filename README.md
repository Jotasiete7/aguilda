# ⚜️ A GUILDA - Códex de Dados NFI 2025

**Status:** ✅ Funcional | ⚠️ CSS com limitações conhecidas

Um sistema de gerenciamento de dados e ferramentas para o jogo **Wurm Online (NFI)**, desenvolvido com React + Vite e design inspirado em "Ancient Book".

---

## 📋 Sobre o Projeto

**A Guilda** é uma aplicação web que centraliza ferramentas e dados essenciais para jogadores de NFI, incluindo:

- 📊 **Dashboard de Preços** - Tabela de preços de mercado atualizada
- 📖 **Guia de Armas** - Informações completas sobre armas, Moon Metals e Enchants
- 🧪 **Receitas** - Sistema de crafting e receitas
- 💼 **Consultoria** - Análise de logs de comércio e relatórios de mercado
- 🗺️ **Harmony SatView** - Visualização de mapas interativos

---

## 🚀 Instalação e Uso

### Pré-requisitos
- **Node.js** (v16 ou superior)
- **npm** ou **yarn**

### Instalação

```bash
# Clone o repositório
git clone https://github.com/Jotasiete7/aguilda.git
cd aguilda

# Instale as dependências
npm install
```

### Executar em Desenvolvimento

```bash
npm run dev
```

O aplicativo estará disponível em `http://localhost:5173`

### Build de Produção

```bash
npm run build
```

Os arquivos otimizados serão gerados na pasta `dist/`.

### Preview do Build

```bash
npm run preview
```

---

## 📁 Estrutura do Projeto

```
aguilda/
├── public/              # Arquivos estáticos
│   ├── consultoria.html # Consultoria (iframe)
│   ├── weapons-guide.html # Guia de Armas (iframe)
│   ├── satview/         # Harmony SatView
│   └── favicon.svg      # Favicon ⚜️
├── src/
│   ├── components/      # Componentes React
│   │   ├── Codex.jsx
│   │   ├── Manifesto.jsx
│   │   └── tools/       # Ferramentas
│   ├── data/            # Dados estáticos
│   ├── index.css        # Estilos globais
│   ├── App.jsx          # Componente principal
│   └── main.jsx         # Entry point
├── dist/                # Build de produção
├── package.json         # Dependências
└── README.md            # Este arquivo
```

---

## ⚠️ Limitações Conhecidas

### CSS Corrompido
Alguns estilos no `index.css` podem apresentar problemas devido a migrações anteriores. O projeto está funcional, mas futuras correções de estilo podem ser necessárias.

### Iframes
As ferramentas **Consultoria** e **Guia de Armas** utilizam iframes apontando para arquivos HTML estáticos para preservar funcionalidades complexas sem corromper o JSX.

---

## 🛠️ Tecnologias Utilizadas

- **React 18** - Biblioteca UI
- **Vite** - Build tool e dev server
- **React Router** - Navegação SPA
- **Lucide React** - Ícones
- **Vanilla CSS** - Estilização customizada

---

## 🎨 Design System

**"Ancient Book"** - Paleta de cores inspirada em livros antigos:

- **Deep Charcoal** (`#0D0D0D`) - Background principal
- **Parchment Gray** (`#E0E0E0`) - Texto principal
- **Fire Red** (`#B00000`) - Acentos e CTAs
- **Soft Gray** (`#737373`) - Texto secundário

---

## 📜 Licença

Este projeto está licenciado sob a **MIT License** - veja o arquivo [LICENSE.md](LICENSE.md) para detalhes.

---

## 🤝 Contribuindo

Contribuições são bem-vindas! Para contribuir:

1. Fork o projeto
2. Crie uma branch para sua feature (`git checkout -b feature/MinhaFeature`)
3. Commit suas mudanças (`git commit -m 'Adiciona MinhaFeature'`)
4. Push para a branch (`git push origin feature/MinhaFeature`)
5. Abra um Pull Request

---

## 📞 Contato

**A Guilda** - [Discord](#) | [GitHub](https://github.com/Jotasiete7/aguilda)

---

**Desenvolvido com ⚜️ por A Guilda**
