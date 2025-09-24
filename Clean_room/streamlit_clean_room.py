import streamlit as st
import pandas as pd
import plotly.express as px
from snowflake.snowpark.context import get_active_session


# Constants (fully qualified names)
XWALK = "CROCEVIA_DB.GOLD_DATA.CRM_CROSSWALK_OVERLAPS"
SUMMIT_ORDERS = "SS_101.HARMONIZED.ORDERS_DT"
CROCEVIA_ORDERS = "CROCEVIA_DB.GOLD_DATA.CC_ORDERS"
ADS = "SS_101.SOURCE_DATA.SOCIAL_AD_IMPRESSIONS"


def run_query(session, sql: str) -> pd.DataFrame:
    try:
        return session.sql(sql).to_pandas()
    except Exception as e:
        with st.expander("Technical details (error)"):
            st.write(sql)
            st.exception(e)
        return pd.DataFrame()


def table_exists(session, fqn: str) -> bool:
    try:
        parts = fqn.split(".")
        if len(parts) != 3:
            return False
        db, schema, table = parts
        df = session.sql(
            f"SELECT 1 FROM {db}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}'"
        ).to_pandas()
        return not df.empty
    except Exception:
        return False


def ui_overview(session):
    st.subheader("Clean Room Overview")

    col1, col2, col3 = st.columns(3)

    # Unique counts
    df_counts = run_query(
        session,
        f"""
        SELECT
          COUNT(DISTINCT SHARED_CUSTOMER_ID) AS SHARED_IDS,
          COUNT(DISTINCT SUMMIT_CUSTOMER_ID) AS SUMMIT_IDS,
          COUNT(DISTINCT CROCEVIA_CUSTOMER_ID) AS CROCEVIA_IDS
        FROM {XWALK}
        """
    )
    if not df_counts.empty:
        col1.metric("Shared identities", int(df_counts.loc[0, "SHARED_IDS"]))
        col2.metric("Summit IDs", int(df_counts.loc[0, "SUMMIT_IDS"]))
        col3.metric("Crocevia IDs", int(df_counts.loc[0, "CROCEVIA_IDS"]))

    df_comp = run_query(
        session,
        f"SELECT OVERLAP_TYPE, COUNT(*) AS PAIRS FROM {XWALK} GROUP BY OVERLAP_TYPE ORDER BY PAIRS DESC"
    )
    with st.expander("Overlap composition (table)", expanded=False):
        st.write(df_comp)

    df_segments = run_query(
        session,
        f"""
        SELECT AUDIENCE_SEGMENT,
               COUNT(DISTINCT SHARED_CUSTOMER_ID) AS CUSTOMERS
        FROM {XWALK}
        GROUP BY AUDIENCE_SEGMENT
        ORDER BY CUSTOMERS DESC
        """
    )
    with st.expander("Audience segments (table)", expanded=False):
        st.write(df_segments)

    # Quick visuals
    c1, c2, c3 = st.columns(3)
    if not df_comp.empty:
        fig_comp = px.pie(df_comp, names="OVERLAP_TYPE", values="PAIRS", title="Overlap Composition")
        c1.plotly_chart(fig_comp, use_container_width=True)
    # More compelling: Value concentration by segment
    df_value = run_query(
        session,
        f"""
        WITH c AS (
          SELECT x.SHARED_CUSTOMER_ID, x.AUDIENCE_SEGMENT, SUM(cco.SALES_PRICE_EURO) AS croc
          FROM {XWALK} x JOIN {CROCEVIA_ORDERS} cco
            ON cco.CUSTOMER_ID = x.CROCEVIA_CUSTOMER_ID
          GROUP BY x.SHARED_CUSTOMER_ID, x.AUDIENCE_SEGMENT
        ), s AS (
          SELECT x.SHARED_CUSTOMER_ID, x.AUDIENCE_SEGMENT, SUM(so.SALES_PRICE_EURO) AS summ
          FROM {XWALK} x JOIN {SUMMIT_ORDERS} so
            ON so.CUSTOMER_ID = x.SUMMIT_CUSTOMER_ID
          GROUP BY x.SHARED_CUSTOMER_ID, x.AUDIENCE_SEGMENT
        )
        SELECT AUDIENCE_SEGMENT,
               ROUND(SUM(COALESCE(summ,0)),0) AS SUMMIT_VALUE,
               ROUND(SUM(COALESCE(croc,0)),0) AS CROCEVIA_VALUE
        FROM (SELECT u.SHARED_CUSTOMER_ID, u.AUDIENCE_SEGMENT, s.summ, c.croc
              FROM {XWALK} u
              LEFT JOIN s ON s.SHARED_CUSTOMER_ID=u.SHARED_CUSTOMER_ID
              LEFT JOIN c ON c.SHARED_CUSTOMER_ID=u.SHARED_CUSTOMER_ID) t
        GROUP BY AUDIENCE_SEGMENT
        ORDER BY SUMMIT_VALUE DESC
        """
    )
    if not df_value.empty:
        fig_val = px.bar(df_value, x="AUDIENCE_SEGMENT", y=["SUMMIT_VALUE", "CROCEVIA_VALUE"], 
                        title="Revenue by Segment (€)", barmode="group")
        fig_val.update_layout(xaxis_title=None, yaxis_title="Revenue (€)")
        c2.plotly_chart(fig_val, use_container_width=True)
    # Geo snapshot
    df_geo = run_query(
        session,
        f"""
        SELECT SUMMIT_POSTAL_CODE AS POSTAL_CODE, COUNT(*) AS CUSTOMERS
        FROM {XWALK}
        WHERE SUMMIT_POSTAL_CODE IS NOT NULL
        GROUP BY SUMMIT_POSTAL_CODE
        ORDER BY CUSTOMERS DESC
        LIMIT 10
        """
    )
    if not df_geo.empty:
        fig_geo = px.bar(df_geo, x="POSTAL_CODE", y="CUSTOMERS", title="Top Areas (Overlapped)")
        fig_geo.update_layout(xaxis_title="Postal Areas", yaxis_title="Customers", xaxis_tickangle=45)
        c3.plotly_chart(fig_geo, use_container_width=True)

    # Age distribution insight (CDO-relevant)
    st.subheader("Customer Demographics & Value Leakage")
    d1, d2 = st.columns(2)
    
    df_age = run_query(
        session,
        f"""
        SELECT
          CASE
            WHEN SUMMIT_BIRTH_DATE IS NULL THEN 'Unknown'
            WHEN DATEDIFF(year, SUMMIT_BIRTH_DATE, CURRENT_DATE) < 25 THEN '<25'
            WHEN DATEDIFF(year, SUMMIT_BIRTH_DATE, CURRENT_DATE) <= 34 THEN '25-34'
            WHEN DATEDIFF(year, SUMMIT_BIRTH_DATE, CURRENT_DATE) <= 44 THEN '35-44'
            WHEN DATEDIFF(year, SUMMIT_BIRTH_DATE, CURRENT_DATE) <= 54 THEN '45-54'
            ELSE '55+'
          END AS AGE_BAND,
          COUNT(DISTINCT SHARED_CUSTOMER_ID) AS CUSTOMERS
        FROM {XWALK}
        GROUP BY 1
        ORDER BY CUSTOMERS DESC
        """
    )
    if not df_age.empty:
        fig_age = px.pie(df_age, names="AGE_BAND", values="CUSTOMERS", title="Age Distribution")
        d1.plotly_chart(fig_age, use_container_width=True)
    
    # Value leakage by segment (more compelling than just counts)
    df_leak_seg = run_query(
        session,
        f"""
        WITH c AS (
          SELECT x.SHARED_CUSTOMER_ID, x.AUDIENCE_SEGMENT, SUM(cco.SALES_PRICE_EURO) AS croc
          FROM {XWALK} x JOIN {CROCEVIA_ORDERS} cco
            ON cco.CUSTOMER_ID = x.CROCEVIA_CUSTOMER_ID
          GROUP BY x.SHARED_CUSTOMER_ID, x.AUDIENCE_SEGMENT
        ), s AS (
          SELECT x.SHARED_CUSTOMER_ID, x.AUDIENCE_SEGMENT, SUM(so.SALES_PRICE_EURO) AS summ
          FROM {XWALK} x JOIN {SUMMIT_ORDERS} so
            ON so.CUSTOMER_ID = x.SUMMIT_CUSTOMER_ID
          GROUP BY x.SHARED_CUSTOMER_ID, x.AUDIENCE_SEGMENT
        )
        SELECT AUDIENCE_SEGMENT,
               ROUND(AVG(COALESCE(croc,0) / NULLIF(COALESCE(summ,0)+COALESCE(croc,0),0)),4) AS AVG_CROCEVIA_SHARE
        FROM (SELECT u.SHARED_CUSTOMER_ID, u.AUDIENCE_SEGMENT, s.summ, c.croc
              FROM {XWALK} u
              LEFT JOIN s ON s.SHARED_CUSTOMER_ID=u.SHARED_CUSTOMER_ID
              LEFT JOIN c ON c.SHARED_CUSTOMER_ID=u.SHARED_CUSTOMER_ID) t
        GROUP BY AUDIENCE_SEGMENT
        ORDER BY AVG_CROCEVIA_SHARE DESC
        """
    )
    if not df_leak_seg.empty:
        fig_leak_seg = px.bar(df_leak_seg, x="AUDIENCE_SEGMENT", y="AVG_CROCEVIA_SHARE", 
                             title="Avg Crocevia Share by Segment", color="AVG_CROCEVIA_SHARE",
                             color_continuous_scale="Reds")
        fig_leak_seg.update_layout(xaxis_title=None, yaxis_title="Crocevia Share", yaxis_tickformat=",.0%")
        d2.plotly_chart(fig_leak_seg, use_container_width=True)

    # Share-of-wallet and potential recapture KPI
    k1, k2, k3 = st.columns(3)
    df_sow = run_query(
        session,
        f"""
        WITH c AS (
          SELECT x.SHARED_CUSTOMER_ID, SUM(cco.SALES_PRICE_EURO) AS croc
          FROM {XWALK} x JOIN {CROCEVIA_ORDERS} cco
            ON cco.CUSTOMER_ID = x.CROCEVIA_CUSTOMER_ID
          GROUP BY x.SHARED_CUSTOMER_ID
        ), s AS (
          SELECT x.SHARED_CUSTOMER_ID, SUM(so.SALES_PRICE_EURO) AS summ
          FROM {XWALK} x JOIN {SUMMIT_ORDERS} so
            ON so.CUSTOMER_ID = x.SUMMIT_CUSTOMER_ID
          GROUP BY x.SHARED_CUSTOMER_ID
        )
        SELECT ROUND(SUM(COALESCE(summ,0)),2) AS summit_rev,
               ROUND(SUM(COALESCE(croc,0)),2) AS crocevia_rev,
               ROUND(AVG(COALESCE(summ,0)/NULLIF(COALESCE(summ,0)+COALESCE(croc,0),0)),4) AS avg_summit_share
        FROM (SELECT u.SHARED_CUSTOMER_ID, s.summ, c.croc
              FROM (SELECT DISTINCT SHARED_CUSTOMER_ID FROM {XWALK}) u
              LEFT JOIN s ON s.SHARED_CUSTOMER_ID=u.SHARED_CUSTOMER_ID
              LEFT JOIN c ON c.SHARED_CUSTOMER_ID=u.SHARED_CUSTOMER_ID) t
        """
    )
    if not df_sow.empty:
        summit_rev = float(df_sow.loc[0, "SUMMIT_REV"])
        croc_rev = float(df_sow.loc[0, "CROCEVIA_REV"])
        avg_share = float(df_sow.loc[0, "AVG_SUMMIT_SHARE"]) if pd.notna(df_sow.loc[0, "AVG_SUMMIT_SHARE"]) else 0.0
        potential_5pct = 0.05 * croc_rev
        k1.metric("Summit revenue (overlap)", f"€{summit_rev:,.0f}")
        k2.metric("Crocevia revenue (overlap)", f"€{croc_rev:,.0f}")
        k3.metric("Avg Summit share of wallet", f"{avg_share*100:.1f}%", help="Avg Summit / (Summit+Crocevia) across overlapped")
        st.info(f"💡 **Opportunity**: If we recapture 5% of Crocevia overlap revenue: +€{potential_5pct:,.0f}")
    
    # Value correlation and recent trends
    st.subheader("Strategic Insights")
    i1, i2 = st.columns(2)
    
    # Correlation
    df_corr = run_query(
        session,
        f"""
        WITH c AS (
          SELECT x.SHARED_CUSTOMER_ID, SUM(cco.SALES_PRICE_EURO) AS croc
          FROM {XWALK} x JOIN {CROCEVIA_ORDERS} cco
            ON cco.CUSTOMER_ID = x.CROCEVIA_CUSTOMER_ID
          GROUP BY x.SHARED_CUSTOMER_ID
        ), s AS (
          SELECT x.SHARED_CUSTOMER_ID, SUM(so.SALES_PRICE_EURO) AS summ
          FROM {XWALK} x JOIN {SUMMIT_ORDERS} so
            ON so.CUSTOMER_ID = x.SUMMIT_CUSTOMER_ID
          GROUP BY x.SHARED_CUSTOMER_ID
        )
        SELECT ROUND(CORR(COALESCE(s.summ,0), COALESCE(c.croc,0)),4) AS SPEND_CORRELATION
        FROM (SELECT DISTINCT SHARED_CUSTOMER_ID FROM {XWALK}) u
        LEFT JOIN s USING (SHARED_CUSTOMER_ID)
        LEFT JOIN c USING (SHARED_CUSTOMER_ID)
        """
    )
    if not df_corr.empty:
        corr_val = float(df_corr.loc[0, "SPEND_CORRELATION"]) if pd.notna(df_corr.loc[0, "SPEND_CORRELATION"]) else 0.0
        i1.metric("Summit ↔ Crocevia spend correlation", f"{corr_val:.3f}", 
                 help="High correlation = customers behave similarly across brands")
    
    # Recent Summit revenue trend among overlapped (last 30 days)
    df_trend = run_query(
        session,
        f"""
        SELECT so.SALE_DATE, ROUND(SUM(so.SALES_PRICE_EURO),0) AS SUMMIT_REVENUE
        FROM {SUMMIT_ORDERS} so
        JOIN {XWALK} x ON so.CUSTOMER_ID = x.SUMMIT_CUSTOMER_ID
        WHERE so.SALE_DATE >= DATEADD(day,-30,CURRENT_DATE)
        GROUP BY so.SALE_DATE
        ORDER BY so.SALE_DATE
        """
    )
    if not df_trend.empty and len(df_trend) > 5:
        fig_trend = px.line(df_trend, x="SALE_DATE", y="SUMMIT_REVENUE", 
                           title="Summit Revenue (Overlapped Customers, Last 30d)")
        fig_trend.update_layout(xaxis_title=None, yaxis_title="Revenue (€)")
        i2.plotly_chart(fig_trend, use_container_width=True)


def ui_audience_builder(session):
    st.subheader("Audience Builder (Clean Room)")
    with st.form("aud_form"):
        overlap_type = st.multiselect("Overlap type", ["BOTH", "EMAIL", "PHONE"], default=["BOTH", "EMAIL", "PHONE"])
        min_summit = st.number_input("Min Summit spend (€)", min_value=0.0, value=0.0, step=10.0)
        min_croc = st.number_input("Min Crocevia spend (€)", min_value=0.0, value=0.0, step=10.0)
        pcode = st.text_input("Filter postal code (starts with)", value="")
        show_ids = st.checkbox("Show identifiers (SUMMIT/CROCEVIA IDs)", value=False)
        submitted = st.form_submit_button("Build audience")

    if not submitted:
        st.info("Configure filters and click 'Build audience'.")
        return

    type_list = ",".join([f"'{t}'" for t in overlap_type]) or "'BOTH','EMAIL','PHONE'"
    escaped_pcode = pcode.replace("'", "''") if pcode else ""
    pcode_filter = f"AND SUMMIT_POSTAL_CODE LIKE '{escaped_pcode}%'" if pcode else ""

    select_ids = "SUMMIT_CUSTOMER_ID, CROCEVIA_CUSTOMER_ID," if show_ids else ""

    sql = f"""
        WITH croc AS (
          SELECT SHARED_CUSTOMER_ID, SUM(TOTAL) AS CROC_REV FROM (
            SELECT x.SHARED_CUSTOMER_ID, SUM(cco.SALES_PRICE_EURO) AS TOTAL
            FROM {XWALK} x
            JOIN {CROCEVIA_ORDERS} cco
              ON cco.CUSTOMER_ID = x.CROCEVIA_CUSTOMER_ID
            GROUP BY x.SHARED_CUSTOMER_ID
          ) t GROUP BY SHARED_CUSTOMER_ID
        ), summ AS (
          SELECT SHARED_CUSTOMER_ID, SUM(TOTAL) AS SUMM_REV FROM (
            SELECT x.SHARED_CUSTOMER_ID, SUM(so.SALES_PRICE_EURO) AS TOTAL
            FROM {XWALK} x
            JOIN {SUMMIT_ORDERS} so
              ON so.CUSTOMER_ID = x.SUMMIT_CUSTOMER_ID
            GROUP BY x.SHARED_CUSTOMER_ID
          ) t GROUP BY SHARED_CUSTOMER_ID
        )
        SELECT {select_ids}
               SHARED_CUSTOMER_ID,
               OVERLAP_TYPE,
               AUDIENCE_SEGMENT,
               SUMMIT_POSTAL_CODE,
               ROUND(COALESCE(s.SUMM_REV,0),2) AS SUMMIT_VALUE,
               ROUND(COALESCE(c.CROC_REV,0),2) AS CROCEVIA_VALUE
        FROM {XWALK} x
        LEFT JOIN summ s USING (SHARED_CUSTOMER_ID)
        LEFT JOIN croc c USING (SHARED_CUSTOMER_ID)
        WHERE OVERLAP_TYPE IN ({type_list})
          AND COALESCE(s.SUMM_REV,0) >= {min_summit}
          AND COALESCE(c.CROC_REV,0) >= {min_croc}
          {pcode_filter}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY SHARED_CUSTOMER_ID ORDER BY COALESCE(s.SUMM_REV,0)+COALESCE(c.CROC_REV,0) DESC)=1
        ORDER BY SUMMIT_VALUE DESC
        LIMIT 500
    """
    df = run_query(session, sql)
    st.dataframe(df, use_container_width=True)

    # Optional: materialize as a view (safer than table) on demand
    with st.expander("Materialize as view (optional)"):
        view_name = st.text_input("View name (in SS_101.SOURCE_DATA)", value="AUDIENCE_VIEW")
        if st.button("Create/Replace View"):
            create_sql = f"CREATE OR REPLACE VIEW SS_101.SOURCE_DATA.{view_name} AS {sql}"
            _ = run_query(session, create_sql)
            st.success(f"Created view SS_101.SOURCE_DATA.{view_name}")

    # Leakage by postcode: high crocevia share and headroom
    st.subheader("Leakage hotspots (postcodes)")
    df_leak = run_query(
        session,
        f"""
        WITH c AS (
          SELECT x.SUMMIT_POSTAL_CODE AS PC, SUM(cco.SALES_PRICE_EURO) AS croc
          FROM {XWALK} x JOIN {CROCEVIA_ORDERS} cco
            ON cco.CUSTOMER_ID = x.CROCEVIA_CUSTOMER_ID
          WHERE x.SUMMIT_POSTAL_CODE IS NOT NULL
          GROUP BY x.SUMMIT_POSTAL_CODE
        ), s AS (
          SELECT x.SUMMIT_POSTAL_CODE AS PC, SUM(so.SALES_PRICE_EURO) AS summ
          FROM {XWALK} x JOIN {SUMMIT_ORDERS} so
            ON so.CUSTOMER_ID = x.SUMMIT_CUSTOMER_ID
          WHERE x.SUMMIT_POSTAL_CODE IS NOT NULL
          GROUP BY x.SUMMIT_POSTAL_CODE
        )
        SELECT COALESCE(s.PC,c.PC) AS POSTAL_CODE,
               ROUND(COALESCE(s.summ,0),2) AS SUMMIT_VALUE,
               ROUND(COALESCE(c.croc,0),2) AS CROCEVIA_VALUE,
               ROUND(COALESCE(c.croc,0) / NULLIF(COALESCE(s.summ,0)+COALESCE(c.croc,0),0),4) AS CROCEVIA_SHARE
        FROM s FULL OUTER JOIN c ON s.PC=c.PC
        WHERE COALESCE(s.summ,0)+COALESCE(c.croc,0) > 0
        ORDER BY CROCEVIA_SHARE DESC NULLS LAST
        LIMIT 15
        """
    )
    if not df_leak.empty:
        fig_leak = px.bar(df_leak, x="POSTAL_CODE", y="CROCEVIA_SHARE", title="Crocevia share by postcode")
        fig_leak.update_layout(yaxis_tickformat=",.0%")
        st.plotly_chart(fig_leak, use_container_width=True)


def ui_examples(session):
    st.subheader("Example: Running shoes at Summit AND Energy drinks at Crocevia")
    with st.form("ex1"):
        summit_pattern = st.text_input("Summit product pattern", value="%running%shoe%")
        croc_pattern = st.text_input("Crocevia product pattern", value="%energy%drink%")
        go = st.form_submit_button("Run example")

    if not go:
        return

    sql = f"""
        WITH summit_running AS (
          SELECT DISTINCT x.SHARED_CUSTOMER_ID
          FROM {SUMMIT_ORDERS} so
          JOIN {XWALK} x ON so.CUSTOMER_ID = x.SUMMIT_CUSTOMER_ID
          WHERE ILIKE(COALESCE(so.PRODUCT_CATEGORY, so.ITEM_CATEGORY, so.PRODUCT_NAME), '{summit_pattern.replace("'","''")}')
        ),
        crocevia_energy AS (
          SELECT DISTINCT x.SHARED_CUSTOMER_ID
          FROM {CROCEVIA_ORDERS} cco
          JOIN {XWALK} x ON cco.CUSTOMER_ID = x.CROCEVIA_CUSTOMER_ID
          WHERE ILIKE(COALESCE(cco.PRODUCT_CATEGORY, cco.ITEM_CATEGORY, cco.PRODUCT_NAME), '{croc_pattern.replace("'","''")}')
        )
        SELECT COUNT(DISTINCT s.SHARED_CUSTOMER_ID) AS CUSTOMERS
        FROM summit_running s
        JOIN crocevia_energy c USING (SHARED_CUSTOMER_ID)
    """
    st.write(run_query(session, sql))

    # Optional ROAS view if ads table exists
    if table_exists(session, ADS):
        st.subheader("ROAS by Platform and Audience Segment (if ads loaded)")
        sql_roas = f"""
        WITH ads AS (
          SELECT PLATFORM, CRM_CUSTOMER_ID, SUM(COST_EUR) AS ad_cost
          FROM {ADS}
          WHERE CRM_CUSTOMER_ID IS NOT NULL
          GROUP BY PLATFORM, CRM_CUSTOMER_ID
        ), rev AS (
          SELECT CUSTOMER_ID, SUM(SALES_PRICE_EURO) AS revenue
          FROM {SUMMIT_ORDERS}
          WHERE CUSTOMER_ID IS NOT NULL
          GROUP BY CUSTOMER_ID
        )
        SELECT a.PLATFORM, x.AUDIENCE_SEGMENT,
               ROUND(SUM(a.ad_cost),2) AS COST,
               ROUND(SUM(r.revenue),2) AS REVENUE,
               ROUND(NULLIF(SUM(r.revenue),0)/NULLIF(SUM(a.ad_cost),0),2) AS ROAS
        FROM ads a
        JOIN {XWALK} x ON a.CRM_CUSTOMER_ID = x.SUMMIT_CUSTOMER_ID
        LEFT JOIN rev r ON r.CUSTOMER_ID = a.CRM_CUSTOMER_ID
        GROUP BY a.PLATFORM, x.AUDIENCE_SEGMENT
        ORDER BY ROAS DESC NULLS LAST
        LIMIT 50
        """
        df_roas = run_query(session, sql_roas)
        st.write(df_roas)


def main():
    st.set_page_config(page_title="Summit x Crocevia Clean Room", layout="wide")

    # Header with logo and title
    h1, h2 = st.columns([1, 3])
    try:
        h1.image("/Users/edendulk/code/crocevia/images/crocevia_logo_horizontal.png", use_container_width=True)
    except Exception:
        pass
    with h2:
        st.title("Summit x Crocevia Clean Room")
        st.caption("Overlap, audiences, and outcomes — privacy‑safe, in Snowflake.")

    session = None
    try:
        session = get_active_session()
    except Exception as e:
        st.error("Could not get Snowflake session.")
        with st.expander("Technical details"):
            st.exception(e)
        return

    tabs = st.tabs(["Overview", "Audience Builder", "Examples"])
    with tabs[0]:
        ui_overview(session)
    with tabs[1]:
        ui_audience_builder(session)
    with tabs[2]:
        ui_examples(session)


main()


