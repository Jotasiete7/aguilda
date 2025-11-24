import React, { useState, useEffect, useMemo } from 'react';
import { dashboardData } from '../../data/dashboardData';
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend,
} from 'chart.js';
import { Bar } from 'react-chartjs-2';

ChartJS.register(
    CategoryScale,
    LinearScale,
    BarElement,
    Title,
    Tooltip,
    Legend
);

const Dashboard = () => {
    const [filterText, setFilterText] = useState('');
    const [categoryFilter, setCategoryFilter] = useState('');
    const [confidenceFilter, setConfidenceFilter] = useState('');
    const [sortConfig, setSortConfig] = useState({ key: 4, direction: 'desc' }); // Default sort by Var%
    const [showOpportunities, setShowOpportunities] = useState(false);

    // Categories for select
    const categories = useMemo(() => {
        return [...new Set(dashboardData.map(row => row[1]))].sort();
    }, []);

    // Filter Data
    const filteredData = useMemo(() => {
        let data = dashboardData.filter(row => {
            const matchesSearch = row[0].toLowerCase().includes(filterText.toLowerCase());
            const matchesCat = !categoryFilter || row[1] === categoryFilter;
            const matchesConf = !confidenceFilter || row[6] === confidenceFilter;
            const matchesOpp = !showOpportunities || (row[5] === "▼" && row[6] === "Alta");
            return matchesSearch && matchesCat && matchesConf && matchesOpp;
        });

        // Sort
        if (sortConfig.key !== null) {
            data.sort((a, b) => {
                let aVal = a[sortConfig.key];
                let bVal = b[sortConfig.key];

                // Handle numeric vs string
                if (typeof aVal === 'string' && sortConfig.key !== 5) { // 5 is indicator symbol
                    aVal = aVal.toLowerCase();
                    bVal = bVal.toLowerCase();
                }

                if (aVal < bVal) return sortConfig.direction === 'asc' ? -1 : 1;
                if (aVal > bVal) return sortConfig.direction === 'asc' ? 1 : -1;
                return 0;
            });
        }
        return data;
    }, [filterText, categoryFilter, confidenceFilter, sortConfig, showOpportunities]);

    // Chart Data (Top 10 Variations)
    const chartData = useMemo(() => {
        const top10 = [...filteredData].sort((a, b) => Math.abs(b[4]) - Math.abs(a[4])).slice(0, 10);
        return {
            labels: top10.map(r => r[0].length > 15 ? r[0].substring(0, 15) + '...' : r[0]),
            datasets: [
                {
                    label: 'Variação %',
                    data: top10.map(r => r[4]),
                    backgroundColor: top10.map(r => r[5] === "▲" ? '#B00000' : r[5] === "▼" ? '#B00000' : '#B00000'),
                },
            ],
        };
    }, [filteredData]);

    // Up Items
    const upItems = useMemo(() => {
        return filteredData.filter(r => r[5] === "▲").slice(0, 8);
    }, [filteredData]);

    const requestSort = (key) => {
        let direction = 'asc';
        if (sortConfig.key === key && sortConfig.direction === 'asc') {
            direction = 'desc';
        }
        setSortConfig({ key, direction });
    };

    const formatPrice = (val) => val > 1000 ? (val / 100).toFixed(2) + "s" : val + "c";

    return (
        <div className="dashboard-container fade-in">
            <div className="dashboard-header">
                <h2 className="section-title">Dashboard de Preços NFI 2025</h2>
                <div className="dashboard-controls">
                    <div className="control-group">
                        <label>Buscar Item</label>
                        <input
                            type="text"
                            placeholder="Ex: Iron Lump..."
                            value={filterText}
                            onChange={(e) => setFilterText(e.target.value)}
                        />
                    </div>
                    <div className="control-group">
                        <label>Categoria</label>
                        <select value={categoryFilter} onChange={(e) => setCategoryFilter(e.target.value)}>
                            <option value="">Todas</option>
                            {categories.map(cat => <option key={cat} value={cat}>{cat}</option>)}
                        </select>
                    </div>
                    <div className="control-group">
                        <label>Confiança</label>
                        <select value={confidenceFilter} onChange={(e) => setConfidenceFilter(e.target.value)}>
                            <option value="">Todas</option>
                            <option value="Alta">Alta</option>
                            <option value="Média">Média</option>
                            <option value="Baixa">Baixa</option>
                        </select>
                    </div>
                    <div className="control-group" style={{ justifyContent: 'flex-end' }}>
                        <button
                            className={`filter-btn ${showOpportunities ? 'active' : ''}`}
                            onClick={() => setShowOpportunities(!showOpportunities)}
                        >
                            {showOpportunities ? 'Mostrar Tudo' : 'Oportunidades de Compra'}
                        </button>
                    </div>
                </div>
            </div>

            <div className="charts-grid">
                <div className="chart-card">
                    <h3>Variação de Preços (Top 10)</h3>
                    <div className="chart-wrapper">
                        <Bar
                            data={chartData}
                            options={{
                                responsive: true,
                                maintainAspectRatio: false,
                                plugins: { legend: { display: false } },
                                scales: {
                                    y: { ticks: { color: '#E0E0E0' }, grid: { color: '#333' } },
                                    x: { ticks: { color: '#E0E0E0' }, grid: { display: false } }
                                }
                            }}
                        />
                    </div>
                </div>
                <div className="chart-card">
                    <h3>Itens em Alta</h3>
                    <div className="items-grid">
                        {upItems.map((r, i) => (
                            <div key={i} className="item-card">
                                <div className="item-header">
                                    <span className="item-name">{r[0]}</span>
                                    <span className="item-trend trend-up">+{r[4]}%</span>
                                </div>
                                <div className="item-stats">
                                    <div className="stat-row">
                                        <span>2025:</span>
                                        <span>{formatPrice(r[3])}</span>
                                    </div>
                                    <div className="stat-row">
                                        <span>Conf:</span>
                                        <span>{r[6]}</span>
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            </div>

            <div className="table-card">
                <h3>Tabela de Preços</h3>
                <div className="table-responsive">
                    <table className="data-table">
                        <thead>
                            <tr>
                                <th onClick={() => requestSort(0)}>Item</th>
                                <th onClick={() => requestSort(1)}>Cat.</th>
                                <th onClick={() => requestSort(2)}>2024</th>
                                <th onClick={() => requestSort(3)}>2025</th>
                                <th onClick={() => requestSort(4)}>Var.</th>
                                <th onClick={() => requestSort(5)}>Ind.</th>
                                <th onClick={() => requestSort(6)}>Conf.</th>
                                <th onClick={() => requestSort(7)}>Venda</th>
                            </tr>
                        </thead>
                        <tbody>
                            {filteredData.map((row, i) => (
                                <tr key={i}>
                                    <td>{row[0]}</td>
                                    <td>{row[1]}</td>
                                    <td>{formatPrice(row[2])}</td>
                                    <td>{formatPrice(row[3])}</td>
                                    <td style={{ color: row[4] > 0 ? '#B00000' : row[4] < 0 ? '#B00000' : '#B00000' }}>
                                        {row[4]}%
                                    </td>
                                    <td style={{ color: row[5] === "▲" ? '#B00000' : row[5] === "▼" ? '#B00000' : '#B00000', fontSize: '1.2rem' }}>
                                        {row[5]}
                                    </td>
                                    <td>{row[6]}</td>
                                    <td>{formatPrice(row[7])}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </div>

            <div className="export-footer" style={{ textAlign: 'center', marginTop: '40px', color: '#737373' }}>
                <small>Atualizado: 31/10/2025 | Feito com amor por <strong>A Guilda</strong></small>
            </div>
        </div>
    );
};

export default Dashboard;
