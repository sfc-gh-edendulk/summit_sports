# 🏃‍♂️ Analytics des Avis Summit Sports

Une application Streamlit complète pour analyser les avis clients en utilisant la fonction AI_AGG de Snowflake. Cette application fournit des insights intelligents sur le sentiment client, les expériences de livraison, les performances des magasins, et plus encore.

## 🌟 Fonctionnalités

### 📊 Tableau de Bord Interactif
- **Métriques en temps réel** : Avis totaux, note moyenne, nombre de magasins, et clients uniques
- **Visualisations magnifiques** : Distributions des notes, tendances dans le temps, et comparaisons de magasins
- **Design responsive** : Fonctionne parfaitement sur ordinateur et mobile

### 🤖 Insights Alimentés par l'IA
- **Analyse Globale du Sentiment** : Résumé complet des expériences clients
- **Analyse des Commentaires Positifs** : Identifie les forces principales et ce que les clients apprécient
- **Axes d'Amélioration** : Recommandations actionnables basées sur les commentaires négatifs
- **Insights Livraison & Logistique** : Analyse focalisée sur les expériences d'expédition et de livraison
- **Analyse Spécifique par Magasin** : Résumés d'expérience client basés sur la localisation

### 📈 Analytics Avancées
- **Tendances des Notes** : Analyse temporelle de la satisfaction client
- **Performance des Magasins** : Comparaison des emplacements par volume d'avis et notes
- **Analyse de la Longueur des Avis** : Corrélation entre le détail des avis et les notes
- **Insights Géographiques** : Analyse des performances par emplacement de magasin

## 🚀 Démarrage Rapide

### Prérequis
- Compte Snowflake avec Streamlit activé
- Accès pour créer des schémas et tables
- Le fichier `social_listening/review_collection/intersport_reviews.csv`

### Instructions de Configuration

#### 1. Configuration Snowflake
```sql
-- Exécutez le script de configuration dans votre worksheet Snowflake
-- Cela crée le schéma requis et la structure de table
USE ROLE ACCOUNTADMIN; -- ou rôle approprié
USE WAREHOUSE COMPUTE_WH; -- ou votre entrepôt préféré
USE DATABASE <VOTRE_BASE_DE_DONNEES>;

-- Exécutez le script setup_snowflake.sql
```

#### 2. Déployer sur Snowflake Streamlit

1. **Créer une nouvelle app Streamlit dans Snowflake :**
   - Allez sur l'interface Web de Snowflake
   - Naviguez vers "Streamlit" dans la barre latérale gauche
   - Cliquez sur "Create Streamlit App"
   - Nommez-la "Summit Sports Analytics FR"

2. **Télécharger les fichiers :**
   - Copiez le contenu de `streamlit_app_fr.py` dans le fichier principal de l'app
   - Téléchargez `environment_fr.yml` pour les dépendances
   - Téléchargez `social_listening/review_collection/intersport_reviews.csv` sur le même stage

3. **Configurer l'app :**
   - Assurez-vous que votre rôle a accès à la base de données
   - Accordez les permissions nécessaires pour la création de schéma

#### 3. Charger les Données

L'app inclut une fonctionnalité de chargement automatique des données :
- Cliquez sur le bouton "🔄 Actualiser les Données" dans la barre latérale
- L'app créera la table et chargera automatiquement les données CSV
- Vous verrez un message de succès quand les données sont chargées

## 📋 Structure de l'App

### Composants Principaux

1. **Onglet Vue d'ensemble** 📊
   - Métriques de performance clés
   - Graphiques de distribution des notes
   - Répartition visuelle de la satisfaction client

2. **Onglet Insights IA** 🤖
   - Alimenté par la fonction AI_AGG de Snowflake
   - Résumés en langage naturel des commentaires clients
   - Intelligence économique actionnable

3. **Onglet Analyse des Magasins** 📍
   - Métriques de performance basées sur la localisation
   - Graphiques de comparaison des magasins
   - Insights géographiques

4. **Onglet Tendances** 📈
   - Analyse temporelle des notes
   - Patterns d'avis dans le temps
   - Analyse de corrélation

## 🛠️ Détails Techniques

### Utilisation de la Fonction AI_AGG

L'app exploite la fonction AI_AGG de Snowflake pour l'analyse intelligente de texte :

```sql
-- Exemple : Analyse globale du sentiment
SELECT AI_AGG(
    REVIEW_TEXT, 
    'Fournissez un résumé complet du sentiment client et des expériences pour ce détaillant de sport'
) as overall_summary
FROM RAW_CUSTOMER.INTERSPORT_REVIEWS;
```

### Patterns SQL Clés

1. **Catégorisation du Sentiment** :
   ```sql
   CASE 
       WHEN RATING >= 4 THEN 'Positif'
       WHEN RATING = 3 THEN 'Neutre'
       WHEN RATING <= 2 THEN 'Négatif'
   END as sentiment_category
   ```

2. **Gestion des Emplacements de Magasins** :
   ```sql
   CASE 
       WHEN STORE_LOCATION IS NULL OR STORE_LOCATION = '' THEN 'Commande en Ligne'
       ELSE STORE_LOCATION 
   END as clean_location
   ```

### Dépendances

- **Streamlit** : Framework d'application web
- **Pandas** : Manipulation et analyse de données
- **Plotly** : Visualisations interactives
- **Snowflake Snowpark** : Connecteur Python pour Snowflake

## 📊 Exemples d'Insights

L'app peut générer des insights comme :

- **"Les clients louent constamment la livraison rapide et les prix compétitifs, avec 89% des avis positifs mentionnant des délais d'expédition rapides."**
- **"Les principaux axes d'amélioration incluent une meilleure gestion des stocks pour réduire les annulations de commandes et des descriptions de produits plus précises sur le site web."**
- **"Les emplacements de magasins dans les grandes villes montrent une satisfaction client supérieure de 15% par rapport aux emplacements plus petits."**

## 🔧 Personnalisation

### Ajouter de Nouveaux Types d'Analyse

Pour ajouter de nouveaux insights IA, modifiez la fonction `get_ai_insights()` :

```python
# Ajouter une nouvelle requête d'insight
new_insight_sql = """
SELECT AI_AGG(
    REVIEW_TEXT,
    'Votre prompt d'analyse personnalisé ici'
) as new_insight
FROM RAW_CUSTOMER.INTERSPORT_REVIEWS
WHERE vos_conditions_ici
"""
```

### Modifier les Visualisations

L'app utilise Plotly pour les graphiques. Personnalisez dans les sections d'onglets respectives :

```python
# Exemple : Modifier le schéma de couleurs
fig = px.bar(data, color_discrete_sequence=['#1f77b4', '#ff7f0e'])
```

## 🚨 Dépannage

### Problèmes Courants

1. **Erreur "Table not found"** :
   - Assurez-vous d'avoir exécuté le script SQL de configuration
   - Vérifiez vos permissions de rôle
   - Vérifiez que le schéma RAW_CUSTOMER existe

2. **Erreurs de fonction AI_AGG** :
   - Assurez-vous d'avoir accès aux fonctions Snowflake Cortex
   - Vérifiez que votre compte Snowflake supporte les fonctions IA
   - Vérifiez qu'il y a suffisamment de données pour l'analyse

3. **Problèmes de chargement de données** :
   - Assurez-vous que le fichier CSV est correctement formaté
   - Vérifiez les permissions de fichier et l'accès
   - Vérifiez que le CSV est au bon emplacement

### Optimisation des Performances

- L'app utilise la mise en cache pour de meilleures performances
- Les grands datasets peuvent nécessiter une optimisation des requêtes
- Considérez l'échantillonnage de données pour de très grands ensembles d'avis

## 📈 Améliorations Futures

Améliorations potentielles :
- **Intégration de données en temps réel** avec les streams Snowflake
- **Analyse NLP avancée** avec des modèles ML personnalisés
- **Analytics prédictives** pour les tendances de satisfaction client
- **Support multilingue** pour les avis internationaux
- **Fonctionnalité d'export** pour les rapports et insights

## 🤝 Contribution

Ceci est une application de démonstration. Pour une utilisation en production :
- Ajouter une gestion d'erreur appropriée
- Implémenter l'authentification utilisateur
- Ajouter la validation des données
- Optimiser pour des datasets plus larges
- Ajouter des tests unitaires

## 📄 Licence

Ce projet est fourni tel quel à des fins de démonstration. Modifiez et utilisez selon vos besoins.

---

Construit avec ❤️ en utilisant Snowflake AI_AGG et Streamlit

## 🇫🇷 Version Française

Cette version française inclut :
- ✅ **Interface complètement traduite** en français
- ✅ **Messages d'erreur et de succès** en français
- ✅ **Titres et étiquettes de graphiques** en français
- ✅ **Instructions utilisateur** en français
- ✅ **Prompts IA optimisés** pour le contenu français
- ✅ **Format de date français** (DD/MM/YYYY)
- ✅ **Fonctionnalité identique** à la version anglaise 