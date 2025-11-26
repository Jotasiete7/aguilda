import React from 'react';
import { Link } from 'react-router-dom';
import { ArrowLeft } from 'lucide-react';

const Consultancy = () => {
    return (
        <div className="consultancy-container fade-in">
            {/* Sidebar */}
            <div className="consultancy-sidebar">
                <Link to="/dashboard" className="btn-back">
                    <ArrowLeft size={16} /> Voltar ao Códex
                </Link>

                <div className="sidebar-header">
                    <span style={{ fontSize: '1.2rem', fontWeight: 'bold' }}>CONSULTORIA</span>
                </div>

                <div className="mt-2">
                    <p className="card-desc">
                        Ferramenta de análise de mercado e logs de comércio.
                    </p>
                </div>
            </div>

            {/* Main Content - Iframe */}
            <div className="consultancy-main">
                <iframe
                    src={`${import.meta.env.BASE_URL}consultoria.html`}
                    title="Consultoria"
                    style={{
                        width: '100%',
                        height: '100%',
                        border: 'none'
                    }}
                />
            </div>
        </div>
    );
};

export default Consultancy;
