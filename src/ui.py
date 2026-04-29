# =========================
# Utilidades UI/UX
# =========================

import streamlit as st


def inject_global_css():
    st.markdown(
        """
        <style>
        /* ===== Base ===== */
        .stApp {
            background: linear-gradient(180deg, #f8fafc 0%, #eef2ff 100%);
        }

        .block-container {
            padding-top: 1.2rem;
            padding-bottom: 1.5rem;
            max-width: 1350px;
        }

        h1, h2, h3 {
            letter-spacing: -0.02em;
        }

        /* ===== Sidebar ===== */
        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%);
            border-right: 1px solid rgba(255,255,255,0.06);
        }

        section[data-testid="stSidebar"] * {
            color: #e2e8f0 !important;
        }

        /* ===== Inputs ===== */
        .stTextInput > div > div > input,
        .stTextArea textarea,
        .stNumberInput input,
        div[data-baseweb="select"] > div {
            border-radius: 12px !important;
            border: 1px solid #cbd5e1 !important;
            background-color: #ffffff !important;
        }

        .stTextInput > label,
        .stTextArea > label,
        .stNumberInput > label {
            font-weight: 600 !important;
        }

        /* ===== Buttons ===== */
        .stButton > button {
            border-radius: 12px !important;
            font-weight: 600 !important;
            border: 1px solid #cbd5e1 !important;
            padding: 0.6rem 1rem !important;
            transition: all 0.2s ease-in-out;
        }

        .stButton > button:hover {
            border-color: #6366f1 !important;
            color: #3730a3 !important;
            box-shadow: 0 4px 14px rgba(99, 102, 241, 0.15);
        }

        /* ===== Dataframe wrapper feel ===== */
        div[data-testid="stDataFrame"] {
            border: 1px solid #e2e8f0;
            border-radius: 14px;
            overflow: hidden;
            background: white;
        }

        /* ===== Tabs ===== */
        .stTabs [data-baseweb="tab-list"] {
            gap: 0.5rem;
        }

        .stTabs [data-baseweb="tab"] {
            border-radius: 12px 12px 0 0;
            padding: 0.7rem 1rem;
            background: #e2e8f0;
        }

        .stTabs [aria-selected="true"] {
            background: #ffffff !important;
            color: #1d4ed8 !important;
            font-weight: 700 !important;
        }

        /* ===== Cards ===== */
        .ux-card {
            background: rgba(255, 255, 255, 0.92);
            border: 1px solid #e2e8f0;
            border-radius: 18px;
            padding: 1rem 1.1rem;
            box-shadow: 0 8px 24px rgba(15, 23, 42, 0.06);
            margin-bottom: 1rem;
        }

        .ux-hero {
            background: linear-gradient(135deg, #1d4ed8 0%, #4338ca 100%);
            color: white;
            border-radius: 20px;
            padding: 1.2rem 1.3rem;
            box-shadow: 0 14px 34px rgba(37, 99, 235, 0.25);
            margin-bottom: 1rem;
        }

        .ux-hero-title {
            font-size: 1.55rem;
            font-weight: 800;
            margin-bottom: 0.3rem;
        }

        .ux-hero-subtitle {
            font-size: 0.98rem;
            opacity: 0.92;
        }

        .ux-metric {
            background: white;
            border: 1px solid #e2e8f0;
            border-radius: 16px;
            padding: 0.95rem 1rem;
            box-shadow: 0 8px 22px rgba(15, 23, 42, 0.05);
        }

        .ux-metric-label {
            font-size: 0.85rem;
            color: #64748b;
            margin-bottom: 0.25rem;
            font-weight: 600;
        }

        .ux-metric-value {
            font-size: 1.45rem;
            font-weight: 800;
            color: #0f172a;
        }

        .ux-badge-ok {
            display: inline-block;
            padding: 0.28rem 0.65rem;
            border-radius: 999px;
            background: #dcfce7;
            color: #166534;
            font-size: 0.82rem;
            font-weight: 700;
        }

        .ux-badge-warn {
            display: inline-block;
            padding: 0.28rem 0.65rem;
            border-radius: 999px;
            background: #fef3c7;
            color: #92400e;
            font-size: 0.82rem;
            font-weight: 700;
        }

        .ux-login-shell {
            max-width: 520px;
            margin: 2rem auto 0 auto;
        }

        .ux-login-card {
            background: rgba(255,255,255,0.96);
            border: 1px solid #e2e8f0;
            border-radius: 22px;
            padding: 1.5rem;
            box-shadow: 0 18px 44px rgba(15, 23, 42, 0.08);
        }

        .ux-login-title {
            font-size: 1.65rem;
            font-weight: 800;
            color: #0f172a;
            text-align: center;
            margin-bottom: 0.25rem;
        }

        .ux-login-subtitle {
            color: #64748b;
            text-align: center;
            margin-bottom: 1rem;
        }

        .ux-note {
            background: #eff6ff;
            border: 1px solid #bfdbfe;
            color: #1e3a8a;
            border-radius: 14px;
            padding: 0.8rem 0.9rem;
            font-size: 0.92rem;
        }

        hr {
            margin-top: 0.9rem !important;
            margin-bottom: 0.9rem !important;
        }

        /* ===== Ocultar marca Streamlit ===== */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        .stAppToolbar {display: none !important;}
        div[data-testid="stDecoration"] {display: none !important;}
        section[data-testid="stSidebar"] > div:first-child > div:first-child {height: 100%;}
        </style>
        """,
        unsafe_allow_html=True,
    )


def hero(title: str, subtitle: str):
    st.markdown(
        f"""
        <div class="ux-hero">
            <div class="ux-hero-title">{title}</div>
            <div class="ux-hero-subtitle">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def card_open():
    st.markdown('<div class="ux-card">', unsafe_allow_html=True)


def card_close():
    st.markdown("</div>", unsafe_allow_html=True)


def metric_card(label: str, value: str):
    st.markdown(
        f"""
        <div class="ux-metric">
            <div class="ux-metric-label">{label}</div>
            <div class="ux-metric-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def badge_ok(text: str):
    st.markdown(
        f'<span class="ux-badge-ok">{text}</span>',
        unsafe_allow_html=True,
    )


def badge_warn(text: str):
    st.markdown(
        f'<span class="ux-badge-warn">{text}</span>',
        unsafe_allow_html=True,
    )
