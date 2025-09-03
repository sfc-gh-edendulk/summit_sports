import streamlit as st
from snowflake.snowpark.context import get_active_session

# Simple test app
st.title("🧪 Simple Snowflake Test")

# Test session
try:
    session = get_active_session()
    st.success("✅ Session connected successfully!")
    
    # Test simple query
    result = session.sql("SELECT CURRENT_VERSION()").collect()
    st.info(f"Snowflake Version: {result[0][0]}")
    
except Exception as e:
    st.error(f"❌ Session error: {e}") 