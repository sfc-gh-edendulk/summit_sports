# 🏃‍♂️ Summit Sports Reviews Analytics

A comprehensive Streamlit application for analyzing customer reviews using Snowflake's AI_AGG function. This app provides intelligent insights into customer sentiment, delivery experiences, store performance, and more.

## 🌟 Features

### 📊 Interactive Dashboard
- **Real-time metrics**: Total reviews, average ratings, store locations, and unique customers
- **Beautiful visualizations**: Rating distributions, trends over time, and store comparisons
- **Responsive design**: Works seamlessly on desktop and mobile devices

### 🤖 AI-Powered Insights
- **Overall Sentiment Analysis**: Comprehensive summary of customer experiences
- **Positive Feedback Analysis**: Identifies top strengths and what customers love
- **Areas for Improvement**: Actionable recommendations based on negative feedback
- **Delivery & Logistics Insights**: Focused analysis on shipping and delivery experiences
- **Store-specific Analysis**: Location-based customer experience summaries

### 📈 Advanced Analytics
- **Rating Trends**: Temporal analysis of customer satisfaction
- **Store Performance**: Compare locations by review volume and ratings
- **Review Length Analysis**: Correlation between review detail and ratings
- **Geographic Insights**: Performance analysis by store location

## 🚀 Quick Start

### Prerequisites
- Snowflake account with Streamlit enabled
- Access to create schemas and tables
- The `social_listening/review_collection/intersport_reviews.csv` file

### Setup Instructions

#### 1. Snowflake Setup
```sql
-- Run the setup script in your Snowflake worksheet
-- This creates the required schema and table structure
USE ROLE ACCOUNTADMIN; -- or appropriate role
USE WAREHOUSE COMPUTE_WH; -- or your preferred warehouse
USE DATABASE <YOUR_DATABASE>;

-- Execute the setup_snowflake.sql script
```

#### 2. Deploy to Snowflake Streamlit

1. **Create a new Streamlit app in Snowflake:**
   - Go to Snowflake Web Interface
   - Navigate to "Streamlit" in the left sidebar
   - Click "Create Streamlit App"
   - Name it "Summit Sports Analytics"

2. **Upload the files:**
   - Copy the contents of `streamlit_app.py` into the main app file
   - Upload `environment.yml` for dependencies
   - Upload `social_listening/review_collection/intersport_reviews.csv` to the same stage

3. **Configure the app:**
   - Ensure your role has access to the SS_101 database
   - Grant necessary permissions for schema creation

#### 3. Load the Data

The app includes an automatic data loading feature:
- Click the "🔄 Refresh Data" button in the sidebar
- The app will create the table and load the CSV data automatically
- You'll see a success message when the data is loaded

## 📋 App Structure

### Main Components

1. **Overview Tab** 📊
   - Key performance metrics
   - Rating distribution charts
   - Visual breakdown of customer satisfaction

2. **AI Insights Tab** 🤖
   - Powered by Snowflake's AI_AGG function
   - Natural language summaries of customer feedback
   - Actionable business intelligence

3. **Store Analysis Tab** 📍
   - Location-based performance metrics
   - Store comparison charts
   - Geographic insights

4. **Trends Tab** 📈
   - Temporal analysis of ratings
   - Review patterns over time
   - Correlation analysis

## 🛠️ Technical Details

### AI_AGG Function Usage

The app leverages Snowflake's AI_AGG function for intelligent text analysis:

```sql
-- Example: Overall sentiment analysis
SELECT AI_AGG(
    REVIEW_TEXT, 
    'Provide a comprehensive summary of customer sentiment and experiences for this sports retailer'
) as overall_summary
FROM SS_101.RAW_CUSTOMER.INTERSPORT_REVIEWS;
```

### Key SQL Patterns

1. **Sentiment Categorization**:
   ```sql
   CASE 
       WHEN RATING >= 4 THEN 'Positive'
       WHEN RATING = 3 THEN 'Neutral'
       WHEN RATING <= 2 THEN 'Negative'
   END as sentiment_category
   ```

2. **Store Location Handling**:
   ```sql
   CASE 
       WHEN STORE_LOCATION IS NULL OR STORE_LOCATION = '' THEN 'Online Order'
       ELSE STORE_LOCATION 
   END as clean_location
   ```

### Dependencies

- **Streamlit**: Web application framework
- **Pandas**: Data manipulation and analysis
- **Plotly**: Interactive visualizations
- **Snowflake Snowpark**: Python connector for Snowflake
- **Snowflake Connector**: Database connectivity

## 📊 Sample Insights

The app can generate insights like:

- **"Customers consistently praise the fast delivery and competitive pricing, with 89% of positive reviews mentioning quick shipping times."**
- **"Main areas for improvement include better inventory management and more accurate product descriptions on the website."**
- **"Store locations in major cities show 15% higher customer satisfaction compared to smaller locations."**

## 🔧 Customization

### Adding New Analysis Types

To add new AI insights, modify the `get_ai_insights()` function:

```python
# Add new insight query
new_insight_sql = """
SELECT AI_AGG(
    REVIEW_TEXT,
    'Your custom analysis prompt here'
) as new_insight
FROM SS_101.RAW_CUSTOMER.INTERSPORT_REVIEWS
WHERE your_conditions_here
"""
```

### Modifying Visualizations

The app uses Plotly for charts. Customize in the respective tab sections:

```python
# Example: Modify color scheme
fig = px.bar(data, color_discrete_sequence=['#1f77b4', '#ff7f0e'])
```

## 🚨 Troubleshooting

### Common Issues

1. **"Table not found" error**:
   - Ensure you've run the setup SQL script
   - Check your role permissions
   - Verify the schema SS_101.RAW_CUSTOMER exists

2. **AI_AGG function errors**:
   - Ensure you have access to Snowflake Cortex functions
   - Check that your Snowflake account supports AI functions
   - Verify there's sufficient data for analysis

3. **Data loading issues**:
   - Ensure the CSV file is properly formatted
   - Check file permissions and access
   - Verify the CSV is in the correct location

### Performance Optimization

- The app uses caching for better performance
- Large datasets may require query optimization
- Consider data sampling for very large review sets

## 📈 Future Enhancements

Potential improvements:
- **Real-time data integration** with Snowflake streams
- **Advanced NLP analysis** with custom ML models
- **Predictive analytics** for customer satisfaction trends
- **Multi-language support** for international reviews
- **Export functionality** for reports and insights

## 🤝 Contributing

This is a demo application. For production use:
- Add proper error handling
- Implement user authentication
- Add data validation
- Optimize for larger datasets
- Add unit tests

## 📄 License

This project is provided as-is for demonstration purposes. Modify and use according to your needs.

---

Built with ❤️ using Snowflake AI_AGG and Streamlit
