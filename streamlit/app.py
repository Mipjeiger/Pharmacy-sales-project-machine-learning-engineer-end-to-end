import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from minio import Minio
from dotenv import load_dotenv
import os
from io import BytesIO
from datetime import datetime

# Load environment variables from .env file
env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=env_path)

# Page configuration
st.set_page_config(
    page_title="Pharmacy Sales Analytics",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown(
    """
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #e0e0e0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .stMetric label {
        color: #262730 !important;
        font-size: 14px !important;
        font-weight: 600 !important;
    }
    .stMetric [data-testid="stMetricValue"] {
        color: #0e1117 !important;
        font-size: 28px !important;
        font-weight: 700 !important;
    }
    .stMetric [data-testid="stMetricDelta"] {
        font-size: 16px !important;
        font-weight: 600 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def get_minio_client():
    """Create MinIO client"""
    try:
        client = Minio(
            f"{os.getenv('minio_host')}:{os.getenv('minio_port')}",
            access_key=os.getenv("access_key"),
            secret_key=os.getenv("secret_key"),
            secure=False,
        )
        return client
    except Exception as e:
        st.error(f"Error creating MinIO client: {e}")
        return None


@st.cache_data(ttl=300)  # Cache data for 5 minutes
def load_data_from_minio(bucket_name, prefix=""):
    """Load all parquet files from a MinIO bucket and return a concatenated DataFrame"""
    client = get_minio_client()
    if not client:
        return None

    try:
        objects = client.list_objects(
            bucket_name=bucket_name, prefix=prefix, recursive=True
        )
        dfs = []

        for obj in objects:
            if obj.object_name.endswith(".parquet"):
                response = client.get_object(
                    bucket_name=bucket_name, object_name=obj.object_name
                )
                data = BytesIO(response.read())
                df = pd.read_parquet(data)
                dfs.append(df)

                # Close the response to free up resources
                response.close()
                response.release_conn()

        if dfs:
            combined_df = pd.concat(dfs, ignore_index=True)
            return combined_df
        else:
            return None

    except Exception as e:
        st.error(f"Error loading data from MinIO: {e}")
        return None


# build function to set as main project UI analytics
def main():
    # Header
    st.title("💊 Pharmacy Sales Analytics Dashboard")
    st.markdown(
        """
        This dashboard provides insights into pharmacy sales data stored in MinIO.
        Use the sidebar to filter data and explore various visualizations.
        """
    )

    # Sidebar
    with st.sidebar:
        st.header("📊 Data Source")

        # Bucket selection
        bucket = st.selectbox(
            "Select MinIO Bucket",
            ["analytics", "gold", "silver", "bronze"],
            index=0,
        )

        # Refresh button
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.markdown("---")
        st.info(
            f"**MinIO Server**: {os.getenv('minio_host', 'localhost')}:{os.getenv('minio_port', '9000')}"
        )
        st.info(f"**Last Updated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load data
    with st.spinner(f"Loading data from {bucket} bucket..."):
        df = load_data_from_minio(bucket)

    if df is None or df.empty:
        st.warning(f"No data found in {bucket} bucket. Please run the pipeline first.")
        st.stop()

    st.success(f"✅ Loaded {len(df):,} records from **{bucket}** bucket")

    # Show available columns for debugging
    with st.expander("🔍 View Available Columns"):
        st.write(df.columns.tolist())

    # Detect data type based on columns
    is_feature_data = "total_sales" in df.columns and "distributor" in df.columns
    is_analytics_data = "product_count" in df.columns

    # Main metrics
    st.header("📈 Key Metrics")

    if is_feature_data:
        # Dynamically create columns based on available data
        metrics = []

        if "total_sales" in df.columns:
            metrics.append(("Total Sales", f"Rp {df['total_sales'].sum():,.0f}"))

        if "total_quantity" in df.columns:
            metrics.append(("Total Quantity", f"{df['total_quantity'].sum():,.0f}"))

        if "avg_price" in df.columns:
            metrics.append(("Average Price", f"Rp {df['avg_price'].mean():,.0f}"))

        if "rolling_avg_3m_sales" in df.columns:
            metrics.append(
                ("Avg 3M Sales", f"Rp {df['rolling_avg_3m_sales'].mean():,.0f}")
            )

        if "sales_growth_pct" in df.columns:
            avg_growth = (
                df["sales_growth_pct"].replace([float("inf"), -float("inf")], 0).mean()
            )
            metrics.append(("Sales Growth", f"{avg_growth:.2f}%"))

        # Display metrics
        cols = st.columns(len(metrics))
        for col, (label, value) in zip(cols, metrics):
            col.metric(label, value)

    elif is_analytics_data:
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric("Total Sales", f"Rp {df['total_sales'].sum():,.0f}")
        with col2:
            st.metric("Total Quantity", f"{df['total_quantity'].sum():,.0f}")
        with col3:
            st.metric("Average Price", f"Rp {df['avg_price'].mean():,.0f}")
        with col4:
            st.metric("Total Products", f"{df['product_count'].sum():,.0f}")

    st.markdown("---")

    # Tabs for different visualizations
    tab1, tab2, tab3, tab4 = st.tabs(
        ["📊 Overview", "🎯 Analysis", "📍 Geographic", "🔍 Details"]
    )

    with tab1:
        st.header("Sales Overview")

        if is_feature_data and "year" in df.columns and "month" in df.columns:
            # Time series
            time_df = (
                df.groupby(["year", "month"])
                .agg({"total_sales": "sum", "total_quantity": "sum"})
                .reset_index()
            )
            time_df["period"] = (
                time_df["year"].astype(str)
                + "-"
                + time_df["month"].astype(str).str.zfill(2)
            )

            col1, col2 = st.columns(2)

            with col1:
                fig = px.line(
                    time_df,
                    x="period",
                    y="total_sales",
                    title="Sales Trend Over Time",
                    markers=True,
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.bar(
                    time_df,
                    x="period",
                    y="total_quantity",
                    title="Quantity Trend Over Time",
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

        # Top distributors
        if "distributor" in df.columns:
            col1, col2 = st.columns(2)

            with col1:
                top_dist = (
                    df.groupby("distributor")["total_sales"]
                    .sum()
                    .nlargest(10)
                    .reset_index()
                )
                fig = px.bar(
                    top_dist,
                    x="total_sales",
                    y="distributor",
                    orientation="h",
                    title="Top 10 Distributors by Sales",
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                if "product_name" in df.columns:
                    top_products = (
                        df.groupby("product_name")["total_sales"]
                        .sum()
                        .nlargest(10)
                        .reset_index()
                    )
                    fig = px.bar(
                        top_products,
                        x="total_sales",
                        y="product_name",
                        orientation="h",
                        title="Top 10 Products by Sales",
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.header("Detailed Analysis")

        # Check if growth columns exist
        if "rolling_avg_3m_sales" in df.columns and "sales_growth_pct" in df.columns:
            # Sales growth analysis
            growth_df = df[df["sales_growth_pct"].notna()].copy()
            # Remove infinite values
            growth_df = growth_df[
                ~growth_df["sales_growth_pct"].isin([float("inf"), -float("inf")])
            ]

            if not growth_df.empty:
                col1, col2 = st.columns(2)

                with col1:
                    fig = px.histogram(
                        growth_df,
                        x="sales_growth_pct",
                        title="Sales Growth Distribution (%)",
                        nbins=50,
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)

                with col2:
                    # Top growing products
                    if "product_name" in growth_df.columns:
                        top_growth = growth_df.nlargest(10, "sales_growth_pct")[
                            ["product_name", "sales_growth_pct"]
                        ]
                        fig = px.bar(
                            top_growth,
                            x="sales_growth_pct",
                            y="product_name",
                            orientation="h",
                            title="Top 10 Growing Products (%)",
                        )
                        fig.update_layout(height=400)
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No valid sales growth data available for visualization.")
        else:
            st.info(
                "Sales growth metrics not available in this bucket. Try selecting 'gold' bucket."
            )

        # Channel analysis
        if "channel" in df.columns:
            col1, col2 = st.columns(2)

            with col1:
                channel_sales = df.groupby("channel")["total_sales"].sum().reset_index()
                fig = px.pie(
                    channel_sales,
                    values="total_sales",
                    names="channel",
                    title="Sales by Channel",
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                if "sub_channel" in df.columns:
                    subchannel_sales = (
                        df.groupby("sub_channel")["total_sales"]
                        .sum()
                        .nlargest(10)
                        .reset_index()
                    )
                    fig = px.bar(
                        subchannel_sales,
                        x="total_sales",
                        y="sub_channel",
                        orientation="h",
                        title="Top 10 Sub-Channels by Sales",
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)

    with tab3:
        st.header("Geographic Analysis")

        if "city" in df.columns:
            city_df = (
                df.groupby("city")
                .agg({"total_sales": "sum", "total_quantity": "sum"})
                .reset_index()
                .nlargest(15, "total_sales")
            )

            col1, col2 = st.columns(2)

            with col1:
                fig = px.bar(
                    city_df,
                    x="total_sales",
                    y="city",
                    orientation="h",
                    title="Top 15 Cities by Sales",
                )
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig = px.scatter(
                    city_df,
                    x="total_quantity",
                    y="total_sales",
                    size="total_sales",
                    hover_data=["city"],
                    title="Sales vs Quantity by City",
                )
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)

            # City heatmap
            if "distributor" in df.columns:
                city_dist = df.pivot_table(
                    values="total_sales",
                    index="city",
                    columns="distributor",
                    aggfunc="sum",
                    fill_value=0,
                )

                fig = go.Figure(
                    data=go.Heatmap(
                        z=city_dist.values,
                        x=city_dist.columns,
                        y=city_dist.index,
                        colorscale="Blues",
                    )
                )
                fig.update_layout(
                    title="Sales Heatmap: City vs Distributor", height=600
                )
                st.plotly_chart(fig, use_container_width=True)

    with tab4:
        st.header("Detailed Data")

        # Filters
        col1, col2, col3 = st.columns(3)

        filtered_df = df.copy()

        with col1:
            if "distributor" in df.columns:
                distributors = st.multiselect(
                    "Filter by Distributor",
                    options=sorted(df["distributor"].unique()),
                    default=None,
                )
                if distributors:
                    filtered_df = filtered_df[
                        filtered_df["distributor"].isin(distributors)
                    ]

        with col2:
            if "city" in df.columns:
                cities = st.multiselect(
                    "Filter by City", options=sorted(df["city"].unique()), default=None
                )
                if cities:
                    filtered_df = filtered_df[filtered_df["city"].isin(cities)]

        with col3:
            if "product_name" in df.columns:
                products = st.multiselect(
                    "Filter by Product",
                    options=sorted(df["product_name"].unique()),
                    default=None,
                )
                if products:
                    filtered_df = filtered_df[
                        filtered_df["product_name"].isin(products)
                    ]

        st.info(f"Showing {len(filtered_df):,} of {len(df):,} records")

        # Display data
        st.dataframe(filtered_df, use_container_width=True, height=500)

        # Download button
        csv = filtered_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"pharmacy_sales_{bucket}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )

    # Footer
    st.markdown("---")
    st.caption(
        f"Dashboard powered by Streamlit | Data from MinIO ({bucket} bucket) | Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )


if __name__ == "__main__":
    main()
