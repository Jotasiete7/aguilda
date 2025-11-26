import React from 'react';
import { Link } from 'react-router-dom';
import { ArrowLeft, Map, Globe, Layers } from 'lucide-react';

const SatView = () => {
    return (
        <div className="satview-container fade-in">
            {/* Sidebar */}
            <div className="satview-sidebar">
                <Link to="/dashboard" className="btn-back">
                    <ArrowLeft size={16} /> Voltar
                </Link>

                <div className="sidebar-header">
                    <Globe size={24} color="#B00000" />
                    <span>HARMONY SATVIEW</span>
                </div>

                <div className="mt-2">
                    <p className="card-desc mb-2">
                        Visualize o terreno de Harmony com dados de satélite em tempo real.
                    </p>

                    <div className="control-group">
                        <label>Camada</label>
                        <select className="sidebar-select">
                            <option>Satélite (Padrão)</option>
                            <option>Topográfico</option>
                            <option>Infraestrutura</option>
                        </select>
                    </div>

                    <ul className="wg-menu mt-2">
                        <li><Map size={14} /> Pontos de Interesse</li>
                        <li><Layers size={14} /> Zonas de Recurso</li>
                    </ul>
                </div>
            </div>

            {/* Main Content - Iframe */}
            <div className="satview-main">
                <iframe
                    src={`${import.meta.env.BASE_URL}satview/index.html`}
                    title="Harmony SatView"
                />
            </div>
        </div>
    );
};

export default SatView;
