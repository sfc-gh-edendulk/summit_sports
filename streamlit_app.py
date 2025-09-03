import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session
import json

# Page configuration
st.set_page_config(
    page_title="🏃‍♂️ Summit Sports Reviews Analytics",
    page_icon="🏃‍♂️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
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

# Remove wrapper function - use get_active_session directly

def create_table_and_upload_data(session, df=None):
    """Create table and upload CSV data"""
    try:
        if session is None:
            return False, "No valid Snowflake session available"
            
        # Create schema if not exists (use current database)
        session.sql("CREATE SCHEMA IF NOT EXISTS RAW_CUSTOMER").collect()
        
        # Create table with fully qualified name
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
        
        # Use provided DataFrame or try to read from file
        if df is None:
            try:
                df = pd.read_csv('summit_sport_reviews.csv')
            except FileNotFoundError:
                return False, "CSV file not found. Please upload a file using the file uploader."
        
        # Validate DataFrame
        if df is None or df.empty:
            return False, "No data found in the uploaded file"
            
        # Convert column names to uppercase to match Snowflake table
        df.columns = df.columns.str.upper()
        
        # Validate required columns
        required_cols = ['CUSTOMER_NAME', 'RATING', 'REVIEW_TEXT', 'DATE', 'STORE_LOCATION']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            return False, f"Missing required columns: {missing_cols}"
        
        # Write to Snowflake with schema specified
        session.write_pandas(df, 'INTERSPORT_REVIEWS', schema='RAW_CUSTOMER', overwrite=True)
        
        return True, len(df)
    except Exception as e:
        import traceback
        error_details = f"Error: {str(e)}\nDetails: {traceback.format_exc()}"
        return False, error_details

def get_basic_stats(session):
    """Get basic statistics from the data"""
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
    """Get rating distribution"""
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

def get_ai_insights(session):
    """Generate AI insights using AI_AGG function - simplified version"""
    insights = {}
    
    # Start with just one simple query to test
    simple_query = """
    SELECT AI_AGG(
        REVIEW_TEXT, 
        'Provide a brief summary of customer sentiment for this sports retailer in 2-3 sentences'
    ) as summary
    FROM RAW_CUSTOMER.INTERSPORT_REVIEWS
    WHERE REVIEW_TEXT IS NOT NULL AND REVIEW_TEXT != ''
    LIMIT 100
    """
    
    try:
        st.info("🔄 Running AI analysis...")
        result = session.sql(simple_query).collect()
        if result and len(result) > 0:
            insights['overall'] = result[0]['SUMMARY']
            st.success("✅ AI analysis completed!")
        else:
            insights['overall'] = "No analysis results returned."
    except Exception as e:
        st.error(f"❌ AI analysis failed: {str(e)}")
        insights['overall'] = f"Analysis failed: {str(e)}"
    
    return insights

def main():
    # Header
    st.markdown('<h1 class="main-header">🏃‍♂️ Summit Sports Reviews Analytics</h1>', unsafe_allow_html=True)
    st.markdown("### Powered by Snowflake AI_AGG Function")
    
    # Test session first
    try:
        session = get_active_session()
        st.success("✅ Connected to Snowflake successfully!")
    except Exception as e:
        st.error(f"❌ Session error: {e}")
        return
    
    # Sidebar
    with st.sidebar:
        st.image("https://via.placeholder.com/200x100/1f77b4/white?text=Summit+Sports", caption="Summit Sports Analytics")
        st.markdown("---")
        
        # File upload widget
        st.markdown("### 📁 Upload Data")
        uploaded_file = st.file_uploader(
            "Upload your reviews CSV file", 
            type=['csv'],
            help="Upload a CSV file with customer reviews data"
        )
        
        # Upload button
        if uploaded_file is not None:
            if st.button("📤 Upload & Process Data", type="primary"):
                with st.spinner("Processing your data..."):
                    try:
                        df = pd.read_csv(uploaded_file)
                        st.info(f"📊 File loaded: {len(df)} reviews found")
                        
                        success, result = create_table_and_upload_data(session, df)
                        if success:
                            st.success(f"✅ Data uploaded successfully! {result} reviews loaded.")
                            st.info("📊 Dashboard will update automatically. Scroll down to see your analytics!")
                        else:
                            st.error(f"❌ Error uploading data: {result}")
                    except Exception as e:
                        st.error(f"❌ Error reading file: {str(e)}")
        
        # Alternative: Refresh existing data
        st.markdown("---")
        if st.button("🔄 Refresh Existing Data"):
            with st.spinner("Refreshing data..."):
                success, result = create_table_and_upload_data(session)
                if success:
                    st.success(f"✅ Data refreshed successfully! {result} reviews loaded.")
                    st.info("📊 Dashboard updated! Check the analytics below.")
                else:
                    st.error(f"❌ Error refreshing data: {result}")
        
        st.markdown("---")
        st.markdown("### 📋 How to Use")
        st.markdown("1. **Upload** your CSV file using the uploader above")
        st.markdown("2. **Click** 'Upload & Process Data' to load it")
        st.markdown("3. **Explore** the insights across different tabs")
        
        st.markdown("### 📊 Dashboard Tabs")
        st.markdown("- 📊 **Overview**: Key metrics and charts")
        st.markdown("- 🤖 **AI Insights**: AI-powered analysis")
        st.markdown("- 📍 **Store Analysis**: Location-based insights")
        st.markdown("- 📈 **Trends**: Rating and temporal analysis")

    # Check if table exists and has data
    try:
        test_query = "SELECT COUNT(*) as count FROM RAW_CUSTOMER.INTERSPORT_REVIEWS"
        result = session.sql(test_query).collect()[0]['COUNT']
        if result == 0:
            st.warning("⚠️ No data found. Please upload your CSV file using the file uploader in the sidebar.")
            st.info("📋 Once you upload data, this dashboard will display comprehensive analytics and AI insights.")
            return
    except Exception as e:
        st.warning("⚠️ Table not found. Please upload your CSV file using the file uploader in the sidebar.")
        st.info("📋 This will create the necessary tables and load your review data for analysis.")
        # Don't show the full error in production, but log it
        with st.expander("🔍 Technical Details (for debugging)"):
            st.code(str(e))
        return

    # Get basic statistics
    stats = get_basic_stats(session)
    
    # Key Metrics Row
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div class="metric-container">
            <h3>{int(stats['TOTAL_REVIEWS'])}</h3>
            <p>Total Reviews</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-container">
            <h3>{stats['AVG_RATING']:.1f}/5</h3>
            <p>Average Rating</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown(f"""
        <div class="metric-container">
            <h3>{int(stats['UNIQUE_STORES'])}</h3>
            <p>Store Locations</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-container">
            <h3>{int(stats['UNIQUE_CUSTOMERS'])}</h3>
            <p>Unique Customers</p>
        </div>
        """, unsafe_allow_html=True)

    # Main content tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🤖 AI Insights", "📍 Store Analysis", "📈 Trends"])
    
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Rating Distribution")
            rating_dist = get_rating_distribution(session)
            
            fig_rating = px.bar(
                rating_dist, 
                x='RATING', 
                y='COUNT',
                title="Distribution of Customer Ratings",
                color='COUNT',
                color_continuous_scale='blues'
            )
            fig_rating.update_layout(
                xaxis_title="Rating",
                yaxis_title="Number of Reviews",
                showlegend=False
            )
            st.plotly_chart(fig_rating, use_container_width=True)
        
        with col2:
            st.subheader("🎯 Rating Breakdown")
            
            # Create a donut chart for ratings
            rating_labels = [f"{row['RATING']} Stars" for _, row in rating_dist.iterrows()]
            
            fig_donut = go.Figure(data=[go.Pie(
                labels=rating_labels,
                values=rating_dist['COUNT'],
                hole=.5,
                marker_colors=['#ff4444', '#ff8800', '#ffcc00', '#88cc00', '#44aa44']
            )])
            
            fig_donut.update_layout(
                title="Rating Distribution (Donut Chart)",
                annotations=[dict(text='Ratings', x=0.5, y=0.5, font_size=20, showarrow=False)]
            )
            st.plotly_chart(fig_donut, use_container_width=True)

    with tab2:
        st.subheader("🤖 AI-Powered Insights")
        
        # Add a manual trigger for AI insights to avoid auto-loading
        if st.button("🧠 Generate AI Insights", type="primary"):
            with st.spinner("Generating insights..."):
                insights = get_ai_insights(session)
            
            if insights:
                # Overall Summary
                if 'overall' in insights and insights['overall']:
                    st.markdown("### 🎯 Overall Customer Sentiment")
                    st.write(insights['overall'])
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if 'positive' in insights and insights['positive']:
                        st.markdown("### ✅ Positive Feedback Analysis")
                        st.write(insights['positive'])
                
                with col2:
                    if 'negative' in insights and insights['negative']:
                        st.markdown("### ❌ Areas for Improvement")
                        st.write(insights['negative'])
                
                # Delivery Insights
                if 'delivery' in insights and insights['delivery']:
                    st.markdown("### 🚚 Delivery & Logistics Insights")
                    st.write(insights['delivery'])
            else:
                st.warning("No insights generated. Please try again.")
        else:
            st.info("👆 Click the button above to generate AI-powered insights from your review data.")

    with tab3:
        st.subheader("📍 Store Location Analysis")
        
        # Top stores by review count
        store_stats_sql = """
        SELECT 
            CASE 
                WHEN STORE_LOCATION IS NULL OR STORE_LOCATION = '' THEN 'Online Orders'
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
            fig_stores = px.bar(
                store_stats.head(8),
                x='REVIEW_COUNT',
                y='STORE',
                orientation='h',
                title="Top Stores by Review Count",
                color='AVG_RATING',
                color_continuous_scale='RdYlGn'
            )
            fig_stores.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_stores, use_container_width=True)
        
        with col2:
            fig_rating_store = px.scatter(
                store_stats,
                x='REVIEW_COUNT',
                y='AVG_RATING',
                size='REVIEW_COUNT',
                hover_data=['STORE'],
                title="Store Performance: Reviews vs Rating",
                color='AVG_RATING',
                color_continuous_scale='RdYlGn'
            )
            st.plotly_chart(fig_rating_store, use_container_width=True)
        
        # Note: AI store analysis can be added later via the AI Insights tab

    with tab4:
        st.subheader("📈 Trends & Patterns")
        
        # Rating trends over time
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
        # Handle DD/MM/YYYY date format from CSV
        temporal_data['DATE'] = pd.to_datetime(temporal_data['DATE'], format='%d/%m/%Y', errors='coerce')
        
        if len(temporal_data) > 1:
            fig_temporal = make_subplots(specs=[[{"secondary_y": True}]])
            
            fig_temporal.add_trace(
                go.Scatter(x=temporal_data['DATE'], y=temporal_data['AVG_RATING'], name="Average Rating"),
                secondary_y=False,
            )
            
            fig_temporal.add_trace(
                go.Bar(x=temporal_data['DATE'], y=temporal_data['REVIEW_COUNT'], name="Review Count", opacity=0.6),
                secondary_y=True,
            )
            
            fig_temporal.update_xaxes(title_text="Date")
            fig_temporal.update_yaxes(title_text="Average Rating", secondary_y=False)
            fig_temporal.update_yaxes(title_text="Review Count", secondary_y=True)
            fig_temporal.update_layout(title_text="Rating Trends Over Time")
            
            st.plotly_chart(fig_temporal, use_container_width=True)
        
        # Word frequency analysis (simple)
        st.markdown("### 📝 Review Length Analysis")
        
        length_sql = """
        SELECT 
            CASE 
                WHEN LENGTH(REVIEW_TEXT) < 50 THEN 'Short (< 50 chars)'
                WHEN LENGTH(REVIEW_TEXT) < 150 THEN 'Medium (50-150 chars)'
                ELSE 'Long (> 150 chars)'
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
                title="Review Length Distribution"
            )
            st.plotly_chart(fig_length, use_container_width=True)
        
        with col2:
            fig_length_rating = px.bar(
                length_data,
                x='REVIEW_LENGTH',
                y='AVG_RATING',
                title="Average Rating by Review Length",
                color='AVG_RATING',
                color_continuous_scale='RdYlGn'
            )
            st.plotly_chart(fig_length_rating, use_container_width=True)

    # Footer
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666;'>
        <p>🏃‍♂️ Summit Sports Analytics Dashboard | Powered by Snowflake AI_AGG & Streamlit</p>
        <p>Last updated: {}</p>
    </div>
    """.format(pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")), unsafe_allow_html=True)

# Run the main function directly
main() 