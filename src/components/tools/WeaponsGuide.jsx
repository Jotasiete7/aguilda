import React from 'react';
import { Link } from 'react-router-dom';
import { ArrowLeft, Sword, Shield, Sparkles, TrendingUp, Book } from 'lucide-react';

const WeaponsGuide = () => {
    return (
        <div className="weapons-guide-container fade-in">
            {/* Sidebar */}
            <div className="wg-sidebar">
                <Link to="/dashboard" className="btn-back">
                    <ArrowLeft size={16} /> Voltar
                </Link>

                <div className="sidebar-header">
                    <Sword size={24} color="#B00000" />
                    <span>GUIA DE ARMAS</span>
                </div>

                <ul className="wg-menu">
                    <li><Book size={14} /> Introdução</li>
                    <li><Sword size={14} /> Tabelas Completas</li>
                    <li><Sparkles size={14} /> Moon Metals</li>
                    <li><Shield size={14} /> Enchants</li>
                    <li><TrendingUp size={14} /> Tier Lists</li>
                </ul>
            </div>

            {/* Main Content - Iframe */}
            <div className="wg-content-wrapper">
                <iframe
                    src={`${import.meta.env.BASE_URL}weapons-guide.html`}
                    title="Weapons Guide"
                    style={{
                        width: '100%',
                        height: '100%',
                        border: 'none',
                        borderRadius: '8px'
                    }}
                />
            </div>
        </div>
    );
};

export default WeaponsGuide;
