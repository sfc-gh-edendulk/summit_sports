import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import json

# Configuration de la page
st.set_page_config(
    page_title="🏃‍♂️ Analytics des Avis Summit Sports",
    page_icon="🏃‍♂️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé pour un meilleur style
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
        background: linear-gradient(90deg, #1f77b4, #ff7f0e);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    
    .metric-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 0.5rem 0;
    }
    
    .insight-box {
        background: #f8f9fa;
        border-left: 4px solid #1f77b4;
        padding: 1rem;
        margin: 1rem 0;
        border-radius: 5px;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
    }
</style>
""", unsafe_allow_html=True)

# Supprimer la fonction wrapper - utiliser get_active_session directement

def create_table_and_upload_data(session, df=None):
    """Créer une table et télécharger les données CSV"""
    try:
        if session is None:
            return False, "Aucune session Snowflake valide disponible"
            
        # Créer le schéma s'il n'existe pas (utiliser la base de données actuelle)
        session.sql("CREATE SCHEMA IF NOT EXISTS RAW_CUSTOMER").collect()
        
        # Créer la table avec un nom complètement qualifié
        create_table_sql = """
        CREATE OR REPLACE TABLE RAW_CUSTOMER.INTERSPORT_REVIEWS (
            CUSTOMER_NAME STRING,
            RATING INTEGER,
            REVIEW_TEXT STRING,
            DATE STRING,
            STORE_LOCATION STRING
        )
        """
        session.sql(create_table_sql).collect()
        
        # Utiliser le DataFrame fourni ou essayer de lire depuis un fichier
        if df is None:
            try:
                df = pd.read_csv('summit_sport_reviews.csv')
            except FileNotFoundError:
                return False, "Fichier CSV introuvable. Veuillez télécharger un fichier en utilisant l'outil de téléchargement."
        
        # Valider le DataFrame
        if df is None or df.empty:
            return False, "Aucune donnée trouvée dans le fichier téléchargé"
            
        # Convertir les noms de colonnes en majuscules pour correspondre à la table Snowflake
        df.columns = df.columns.str.upper()
        
        # Valider les colonnes requises
        required_cols = ['CUSTOMER_NAME', 'RATING', 'REVIEW_TEXT', 'DATE', 'STORE_LOCATION']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, f"Colonnes requises manquantes : {missing_cols}"
        
        # Écrire vers Snowflake avec le schéma spécifié
        session.write_pandas(df, 'INTERSPORT_REVIEWS', schema='RAW_CUSTOMER', overwrite=True)
        
        return True, len(df)
    except Exception as e:
        import traceback
        error_details = f"Erreur : {str(e)}\nDétails : {traceback.format_exc()}"
        return False, error_details

def get_basic_stats(session):
    """Obtenir les statistiques de base des données"""
    stats_sql = """
    SELECT 
        COUNT(*) as total_reviews,
        AVG(RATING) as avg_rating,
        COUNT(DISTINCT STORE_LOCATION) as unique_stores,
        COUNT(DISTINCT CUSTOMER_NAME) as unique_customers,
        MIN(DATE) as earliest_review,
        MAX(DATE) as latest_review
    FROM RAW_CUSTOMER.INTERSPORT_REVIEWS
    WHERE RATING IS NOT NULL
    """
    return session.sql(stats_sql).to_pandas().iloc[0]

def get_rating_distribution(session):
    """Obtenir la distribution des notes"""
    rating_sql = """
    SELECT 
        RATING,
        COUNT(*) as count
    FROM RAW_CUSTOMER.INTERSPORT_REVIEWS
    WHERE RATING IS NOT NULL
    GROUP BY RATING
    ORDER BY RATING
    """
    return session.sql(rating_sql).to_pandas()

def get_sample_reviews(session, rating_filter=None, store_filter=None, limit=5):
    """Obtenir un échantillon d'avis avec filtres optionnels"""
    where_conditions = ["REVIEW_TEXT IS NOT NULL", "REVIEW_TEXT != ''"]
    
    if rating_filter:
        where_conditions.append(f"RATING = {rating_filter}")
    if store_filter and store_filter != "Tous":
        where_conditions.append(f"STORE_LOCATION = '{store_filter}'")
    
    where_clause = " AND ".join(where_conditions)
    
    sample_sql = f"""
    SELECT 
        CUSTOMER_NAME,
        RATING,
        REVIEW_TEXT,
        DATE,
        STORE_LOCATION,
        LENGTH(REVIEW_TEXT) as review_length
    FROM RAW_CUSTOMER.INTERSPORT_REVIEWS
    WHERE {where_clause}
    ORDER BY RANDOM()
    LIMIT {limit}
    """
    
    return session.sql(sample_sql).to_pandas()

def display_review_preview(review_row, show_full=False):
    """Afficher un aperçu stylé d'un avis"""
    rating_stars = "⭐" * int(review_row['RATING']) + "☆" * (5 - int(review_row['RATING']))
    
    # Tronquer le texte si nécessaire
    review_text = review_row['REVIEW_TEXT']
    if not show_full and len(review_text) > 150:
        review_text = review_text[:150] + "..."
    
    store_name = review_row['STORE_LOCATION'] if review_row['STORE_LOCATION'] else "Commande en ligne"
    
    st.markdown(f"""
    <div style="border: 1px solid #e0e0e0; border-radius: 8px; padding: 15px; margin: 10px 0; background: #f9f9f9;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
            <strong>👤 {review_row['CUSTOMER_NAME']}</strong>
            <span style="font-size: 18px;">{rating_stars}</span>
        </div>
        <p style="margin: 10px 0; line-height: 1.4;">{review_text}</p>
        <div style="display: flex; justify-content: space-between; font-size: 12px; color: #666;">
            <span>📍 {store_name}</span>
            <span>📅 {review_row['DATE']}</span>
        </div>
    </div>
    """, unsafe_allow_html=True)

def get_ai_insights(session):
    """Générer des insights IA en utilisant la fonction AI_AGG - version simplifiée"""
    insights = {}
    
    # Définir le prompt séparément pour éviter les problèmes d'échappement
    ai_prompt = "Vous faites du social listening et conseil strategique pour Intersport. Fournissez un resume du sentiment client d'apres des avis recueillis. Soulignez des points forts et axes d'amelioration et puis 3-5 highlights strategiques"
    
    # Construire la requête avec le prompt
    simple_query = f"""
    SELECT AI_AGG(
        REVIEW_TEXT, 
        $${ai_prompt}$$
    ) as summary
    FROM RAW_CUSTOMER.INTERSPORT_REVIEWS
    WHERE REVIEW_TEXT IS NOT NULL AND REVIEW_TEXT != ''
    LIMIT 100
    """
    
    try:
        st.info("🔄 Exécution de l'analyse IA...")
        result = session.sql(simple_query).collect()
        if result and len(result) > 0:
            insights['overall'] = result[0]['SUMMARY']
            st.success("✅ Analyse IA terminée !")
        else:
            insights['overall'] = "Aucun résultat d'analyse retourné."
    except Exception as e:
        st.error(f"❌ L'analyse IA a échoué : {str(e)}")
        insights['overall'] = f"L'analyse a échoué : {str(e)}"
    
    return insights

def main():
    # En-tête
    st.markdown('<h1 class="main-header">🏃‍♂️ Analytics des Avis Summit Sports</h1>', unsafe_allow_html=True)
    st.markdown("### Alimenté par la fonction AI_AGG de Snowflake")
    
    # Tester la session en premier
    try:
        session = get_active_session()
        st.success("✅ Connecté à Snowflake avec succès !")
    except Exception as e:
        st.error(f"❌ Erreur de session : {e}")
        return
    
    # Barre latérale
    with st.sidebar:
        st.image("https://via.placeholder.com/200x100/1f77b4/white?text=Summit+Sports", caption="Analytics Summit Sports")
        st.markdown("---")
        
        # Widget de téléchargement de fichier
        st.markdown("### 📁 Télécharger les Données")
        uploaded_file = st.file_uploader(
            "Téléchargez votre fichier CSV d'avis", 
            type=['csv'],
            help="Téléchargez un fichier CSV avec les données d'avis clients"
        )
        
        # Bouton de téléchargement
        if uploaded_file is not None:
            if st.button("📤 Télécharger et Traiter les Données", type="primary"):
                with st.spinner("Traitement de vos données..."):
                    try:
                        df = pd.read_csv(uploaded_file)
                        st.info(f"📊 Fichier chargé : {len(df)} avis trouvés")
                        
                        success, result = create_table_and_upload_data(session, df)
                        if success:
                            st.success(f"✅ Données téléchargées avec succès ! {result} avis chargés.")
                            st.info("📊 Le tableau de bord se mettra à jour automatiquement. Faites défiler vers le bas pour voir vos analytics !")
                        else:
                            st.error(f"❌ Erreur lors du téléchargement des données : {result}")
                    except Exception as e:
                        st.error(f"❌ Erreur lors de la lecture du fichier : {str(e)}")
        
        # Alternative : Actualiser les données existantes
        st.markdown("---")
        if st.button("🔄 Actualiser les Données Existantes"):
            with st.spinner("Actualisation des données..."):
                success, result = create_table_and_upload_data(session)
                if success:
                    st.success(f"✅ Données actualisées avec succès ! {result} avis chargés.")
                    st.info("📊 Tableau de bord mis à jour ! Consultez les analytics ci-dessous.")
                else:
                    st.error(f"❌ Erreur lors de l'actualisation des données : {result}")
        
        st.markdown("---")
        st.markdown("### 📋 Comment Utiliser")
        st.markdown("1. **Téléchargez** votre fichier CSV en utilisant l'outil ci-dessus")
        st.markdown("2. **Cliquez** sur 'Télécharger et Traiter les Données' pour le charger")
        st.markdown("3. **Explorez** les insights dans les différents onglets")
        
        st.markdown("### 📊 Onglets du Tableau de Bord")
        st.markdown("- 📊 **Vue d'ensemble & Insights** : Métriques clés, graphiques et analyse IA")
        st.markdown("- 📈 **Analyse & Tendances** : Insights magasins et analyse temporelle")

    # Vérifier si la table existe et contient des données
    try:
        test_query = "SELECT COUNT(*) as count FROM RAW_CUSTOMER.INTERSPORT_REVIEWS"
        result = session.sql(test_query).collect()[0]['COUNT']
        if result == 0:
            st.warning("⚠️ Aucune donnée trouvée. Veuillez télécharger votre fichier CSV en utilisant l'outil de téléchargement dans la barre latérale.")
            st.info("📋 Une fois que vous téléchargez les données, ce tableau de bord affichera des analytics complets et des insights IA.")
            return
    except Exception as e:
        st.warning("⚠️ Table non trouvée. Veuillez télécharger votre fichier CSV en utilisant l'outil de téléchargement dans la barre latérale.")
        st.info("📋 Cela créera les tables nécessaires et chargera vos données d'avis pour l'analyse.")
        # Ne pas afficher l'erreur complète en production, mais la journaliser
        with st.expander("🔍 Détails Techniques (pour le débogage)"):
            st.code(str(e))
        return

    # Obtenir les statistiques de base
    stats = get_basic_stats(session)
    
    # Ligne des métriques clés
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="metric-container">
            <h3>{int(stats['TOTAL_REVIEWS'])}</h3>
            <p>Avis Totaux</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-container">
            <h3>{stats['AVG_RATING']:.1f}/5</h3>
            <p>Note Moyenne</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-container">
            <h3>{int(stats['UNIQUE_STORES'])}</h3>
            <p>Emplacements de Magasins</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-container">
            <h3>{int(stats['UNIQUE_CUSTOMERS'])}</h3>
            <p>Clients Uniques</p>
        </div>
        """, unsafe_allow_html=True)

    # Section d'aperçu des avis récents
    st.markdown("---")
    st.subheader("💬 Aperçu des Avis Récents")
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("Découvrez ce que disent vos clients")
    with col2:
        if st.button("🔄 Actualiser les avis", key="refresh_preview"):
            st.rerun()
    
    try:
        recent_reviews = get_sample_reviews(session, limit=3)
        if not recent_reviews.empty:
            cols = st.columns(3)
            for idx, (_, review) in enumerate(recent_reviews.iterrows()):
                with cols[idx]:
                    display_review_preview(review)
        else:
            st.info("Aucun avis disponible pour l'aperçu.")
    except Exception as e:
        st.warning("Impossible de charger les avis récents.")

    # Onglets de contenu principal (combinés)
    tab1, tab2 = st.tabs(["📊 Vue d'ensemble & Insights", "📈 Analyse & Tendances"])
    
    with tab1:
        st.subheader("📊 Vue d'ensemble des Données")
        
        # Section des graphiques de distribution
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📊 Distribution des Notes")
            rating_dist = get_rating_distribution(session)
            
            fig_rating = px.bar(
                rating_dist, 
                x='RATING', 
                y='COUNT',
                title="Distribution des Notes Clients",
                color='COUNT',
                color_continuous_scale='blues'
            )
            fig_rating.update_layout(
                xaxis_title="Note",
                yaxis_title="Nombre d'Avis",
                showlegend=False
            )
            st.plotly_chart(fig_rating, use_container_width=True)
        
        with col2:
            st.markdown("#### 🎯 Répartition des Notes")
            
            # Créer un graphique en anneau pour les notes
            rating_labels = [f"{row['RATING']} Étoiles" for _, row in rating_dist.iterrows()]
            
            fig_donut = go.Figure(data=[go.Pie(
                labels=rating_labels,
                values=rating_dist['COUNT'],
                hole=.5,
                marker_colors=['#ff4444', '#ff8800', '#ffcc00', '#88cc00', '#44aa44']
            )])
            
            fig_donut.update_layout(
                title="Distribution des Notes (Graphique en Anneau)",
                annotations=[dict(text='Notes', x=0.5, y=0.5, font_size=20, showarrow=False)]
            )
            st.plotly_chart(fig_donut, use_container_width=True)

        # Aperçu des avis par note
        st.markdown("---")
        st.markdown("#### 💬 Aperçu des Avis par Note")
        
        rating_cols = st.columns(5)
        for i, rating in enumerate([5, 4, 3, 2, 1]):
            with rating_cols[i]:
                st.markdown(f"**{rating} ⭐**")
                try:
                    sample_for_rating = get_sample_reviews(session, rating_filter=rating, limit=1)
                    if not sample_for_rating.empty:
                        review = sample_for_rating.iloc[0]
                        short_text = review['REVIEW_TEXT'][:100] + "..." if len(review['REVIEW_TEXT']) > 100 else review['REVIEW_TEXT']
                        store_name = review['STORE_LOCATION'] if review['STORE_LOCATION'] else "En ligne"
                        st.markdown(f"""
                        <div style="font-size: 11px; padding: 8px; background: #f0f0f0; border-radius: 5px; margin: 5px 0;">
                            <em>"{short_text}"</em><br/>
                            <small>👤 {review['CUSTOMER_NAME']} - 📍 {store_name}</small>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.markdown("*Aucun avis disponible*")
                except:
                    st.markdown("*Aucun avis disponible*")

        # Section des Insights IA
        st.markdown("---")
        st.subheader("🤖 Insights Alimentés par l'IA")
        
        col1, col2 = st.columns([2, 1])
        with col1:
            st.markdown("Obtenez des insights automatiques générés par l'IA de Snowflake")
        with col2:
            generate_ai = st.button("🧠 Générer des Insights IA", type="primary")
        
        if generate_ai:
            with st.spinner("Génération des insights..."):
                insights = get_ai_insights(session)
            
            if insights:
                # Résumé global
                if 'overall' in insights and insights['overall']:
                    st.markdown("#### 🎯 Sentiment Client Global")
                    st.markdown(f"""
                    <div class="insight-box">
                        {insights['overall']}
                    </div>
                    """, unsafe_allow_html=True)
                
                # Avis détaillés pour contexte
                st.markdown("#### 📝 Exemples d'Avis Analysés")
                col1, col2 = st.columns(2)
                
                try:
                    # Avis positifs
                    with col1:
                        st.markdown("**✅ Avis Positifs (4-5 ⭐)**")
                        positive_reviews = get_sample_reviews(session, rating_filter=5, limit=2)
                        if not positive_reviews.empty:
                            for _, review in positive_reviews.iterrows():
                                display_review_preview(review, show_full=False)
                    
                    # Avis critiques
                    with col2:
                        st.markdown("**⚠️ Avis à Améliorer (1-2 ⭐)**")
                        negative_reviews = get_sample_reviews(session, rating_filter=1, limit=2)
                        if negative_reviews.empty:
                            negative_reviews = get_sample_reviews(session, rating_filter=2, limit=2)
                        
                        if not negative_reviews.empty:
                            for _, review in negative_reviews.iterrows():
                                display_review_preview(review, show_full=False)
                except:
                    st.info("Impossible de charger les exemples d'avis.")
            else:
                st.warning("Aucun insight généré. Veuillez réessayer.")
        else:
            st.info("👆 Cliquez sur le bouton ci-dessus pour générer des insights alimentés par l'IA à partir de vos données d'avis.")

    with tab2:
        st.subheader("📍 Analyse des Magasins")
        
        # Top des magasins par nombre d'avis
        store_stats_sql = """
        SELECT 
            CASE 
                WHEN STORE_LOCATION IS NULL OR STORE_LOCATION = '' THEN 'Commandes en Ligne'
                ELSE STORE_LOCATION 
            END as store,
            COUNT(*) as review_count,
            AVG(RATING) as avg_rating
        FROM RAW_CUSTOMER.INTERSPORT_REVIEWS
        WHERE RATING IS NOT NULL
        GROUP BY store
        ORDER BY review_count DESC
        LIMIT 10
        """
        
        store_stats = session.sql(store_stats_sql).to_pandas()
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🏪 Top des Magasins par Avis")
            fig_stores = px.bar(
                store_stats.head(8),
                x='REVIEW_COUNT',
                y='STORE',
                orientation='h',
                title="Top des Magasins par Nombre d'Avis",
                color='AVG_RATING',
                color_continuous_scale='RdYlGn'
            )
            fig_stores.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_stores, use_container_width=True)
        
        with col2:
            st.markdown("#### 📈 Performance des Magasins")
            fig_rating_store = px.scatter(
                store_stats,
                x='REVIEW_COUNT',
                y='AVG_RATING',
                size='REVIEW_COUNT',
                hover_data=['STORE'],
                title="Performance des Magasins : Avis vs Note",
                color='AVG_RATING',
                color_continuous_scale='RdYlGn'
            )
            st.plotly_chart(fig_rating_store, use_container_width=True)

        # Aperçu des avis par magasin
        if not store_stats.empty:
            st.markdown("---")
            st.markdown("#### 💬 Aperçu des Avis par Magasin")
            
            # Sélecteur de magasin
            selected_store = st.selectbox(
                "Choisissez un magasin pour voir les avis:",
                ["Tous"] + store_stats['STORE'].tolist()
            )
            
            if selected_store != "Tous":
                try:
                    store_reviews = get_sample_reviews(session, store_filter=selected_store, limit=3)
                    if not store_reviews.empty:
                        cols = st.columns(min(3, len(store_reviews)))
                        for idx, (_, review) in enumerate(store_reviews.iterrows()):
                            with cols[idx % 3]:
                                display_review_preview(review)
                    else:
                        st.info(f"Aucun avis trouvé pour {selected_store}")
                except:
                    st.warning("Impossible de charger les avis pour ce magasin.")

        # Section des Tendances
        st.markdown("---")
        st.subheader("📈 Tendances et Patterns")
        
        # Tendances des notes dans le temps
        temporal_sql = """
        SELECT 
            DATE,
            AVG(RATING) as avg_rating,
            COUNT(*) as review_count
        FROM RAW_CUSTOMER.INTERSPORT_REVIEWS
        WHERE RATING IS NOT NULL AND DATE IS NOT NULL
        GROUP BY DATE
        ORDER BY DATE
        """
        
        temporal_data = session.sql(temporal_sql).to_pandas()
        # Gérer le format de date DD/MM/YYYY du CSV
        temporal_data['DATE'] = pd.to_datetime(temporal_data['DATE'], format='%d/%m/%Y', errors='coerce')
        
        if len(temporal_data) > 1:
            st.markdown("#### 📊 Évolution des Notes dans le Temps")
            fig_temporal = make_subplots(specs=[[{"secondary_y": True}]])
            
            fig_temporal.add_trace(
                go.Scatter(x=temporal_data['DATE'], y=temporal_data['AVG_RATING'], name="Note Moyenne"),
                secondary_y=False,
            )
            
            fig_temporal.add_trace(
                go.Bar(x=temporal_data['DATE'], y=temporal_data['REVIEW_COUNT'], name="Nombre d'Avis", opacity=0.6),
                secondary_y=True,
            )
            
            fig_temporal.update_xaxes(title_text="Date")
            fig_temporal.update_yaxes(title_text="Note Moyenne", secondary_y=False)
            fig_temporal.update_yaxes(title_text="Nombre d'Avis", secondary_y=True)
            fig_temporal.update_layout(title_text="Tendances des Notes dans le Temps")
            
            st.plotly_chart(fig_temporal, use_container_width=True)
        
        # Analyse de la longueur des avis
        st.markdown("---")
        st.markdown("#### 📝 Analyse de la Longueur des Avis")
        
        length_sql = """
        SELECT 
            CASE 
                WHEN LENGTH(REVIEW_TEXT) < 50 THEN 'Court (< 50 caractères)'
                WHEN LENGTH(REVIEW_TEXT) < 150 THEN 'Moyen (50-150 caractères)'
                ELSE 'Long (> 150 caractères)'
            END as review_length,
            COUNT(*) as count,
            AVG(RATING) as avg_rating
        FROM RAW_CUSTOMER.INTERSPORT_REVIEWS
        WHERE REVIEW_TEXT IS NOT NULL AND REVIEW_TEXT != ''
        GROUP BY review_length
        """
        
        length_data = session.sql(length_sql).to_pandas()
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig_length = px.pie(
                length_data,
                values='COUNT',
                names='REVIEW_LENGTH',
                title="Distribution de la Longueur des Avis"
            )
            st.plotly_chart(fig_length, use_container_width=True)
        
        with col2:
            fig_length_rating = px.bar(
                length_data,
                x='REVIEW_LENGTH',
                y='AVG_RATING',
                title="Note Moyenne par Longueur d'Avis",
                color='AVG_RATING',
                color_continuous_scale='RdYlGn'
            )
            st.plotly_chart(fig_length_rating, use_container_width=True)

        # Aperçu des avis longs vs courts
        st.markdown("---")
        st.markdown("#### 📖 Exemples d'Avis par Longueur")
        
        col1, col2 = st.columns(2)
        
        try:
            # Avis courts
            with col1:
                st.markdown("**Avis Courts (< 50 caractères)**")
                short_reviews_sql = """
                SELECT * FROM RAW_CUSTOMER.INTERSPORT_REVIEWS
                WHERE LENGTH(REVIEW_TEXT) < 50 AND REVIEW_TEXT IS NOT NULL
                ORDER BY RANDOM() LIMIT 2
                """
                short_reviews = session.sql(short_reviews_sql).to_pandas()
                if not short_reviews.empty:
                    for _, review in short_reviews.iterrows():
                        display_review_preview(review)
            
            # Avis longs
            with col2:
                st.markdown("**Avis Détaillés (> 150 caractères)**")
                long_reviews_sql = """
                SELECT * FROM RAW_CUSTOMER.INTERSPORT_REVIEWS
                WHERE LENGTH(REVIEW_TEXT) > 150 AND REVIEW_TEXT IS NOT NULL
                ORDER BY RANDOM() LIMIT 2
                """
                long_reviews = session.sql(long_reviews_sql).to_pandas()
                if not long_reviews.empty:
                    for _, review in long_reviews.iterrows():
                        display_review_preview(review)
        except:
            st.info("Impossible de charger les exemples d'avis par longueur.")

    # Pied de page
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <p>🏃‍♂️ Tableau de Bord Analytics Summit Sports | Alimenté par Snowflake AI_AGG & Streamlit</p>
        <p>Dernière mise à jour : {}</p>
    </div>
    """.format(pd.Timestamp.now().strftime("%d/%m/%Y %H:%M:%S")), unsafe_allow_html=True)

# Exécuter la fonction principale directement
main() 