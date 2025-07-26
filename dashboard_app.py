import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark import Session
import toml

# --- Page Configuration ---
st.set_page_config(
    page_title="M5 Supply Chain Dashboard",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Snowflake Connection ---
# Use st.cache_resource to only create the connection once

# @st.cache_resource
# def create_session():
#     try:
#         with open('config.toml', 'r') as f:
#             connection_params = toml.load(f)['connection']
#         session = Session.builder.configs(connection_params).create()
#         print("New Snowpark session created.")
#         return session
#     except Exception as e:
#         st.error(f"Error connecting to Snowflake: {e}")
#         return None




@st.cache_resource
def create_session():
    # First, try to connect using Streamlit's secrets management for cloud deployment
    try:
        # st.secrets reads from the secrets you will set up on the Streamlit Cloud website
        connection_params = st.secrets["snowflake"]
        session = Session.builder.configs(connection_params).create()
        print("Connected to Snowflake using Streamlit secrets.")
        return session
    except Exception as e1:
        # If cloud secrets aren't found, fall back to the local config.toml for local development
        print(f"Streamlit secrets not found ({e1}), trying local config.toml.")
        try:
            with open('config.toml', 'r') as f:
                connection_params = toml.load(f)['connection']
            session = Session.builder.configs(connection_params).create()
            print("Connected to Snowflake using local config.toml.")
            return session
        except Exception as e2:
            st.error(f"Failed to connect to Snowflake. Error: {e2}")
            return None
        
        
# session = create_session()

if not session:
    st.info("Please check your Snowflake connection configuration in config.toml.")
    st.stop()


# --- Data Loading ---
# Use st.cache_data to load data and cache it for performance.
# The cache will be cleared every hour (3600 seconds).
@st.cache_data(ttl=3600)
def load_data(_session):
    # Load the forecast data
    forecast_df = _session.table("M5_DEMAND_FORECASTS").to_pandas()
    forecast_df['FULL_DATE'] = pd.to_datetime(forecast_df['FULL_DATE'])
    
    # Load the inventory recommendations
    inventory_df = _session.table("M5_INVENTORY_RECOMMENDATIONS").to_pandas()

    return forecast_df, inventory_df

try:
    forecast_data, inventory_data = load_data(session)
    st.success("Successfully loaded data from Snowflake!")
except Exception as e:
    st.error(f"An error occurred while loading data: {e}")
    st.info("Please ensure that the ML pipeline has been run successfully and the tables 'M5_DEMAND_FORECASTS' and 'M5_INVENTORY_RECOMMENDATIONS' exist in your DBT_DEV schema.")
    st.stop()


# --- Dashboard UI ---
st.title("📦 M5 Demand & Inventory Dashboard")
st.markdown("An interactive dashboard to explore sales forecasts and inventory recommendations.")

# --- Sidebar Filters ---
st.sidebar.header("Filters")
# Using the raw ID for simplicity. In a real app, you might parse this.
# Let's get a smaller, unique list of IDs for the selectbox to keep it fast.
unique_ids = forecast_data['ID'].unique()
selected_id = st.sidebar.selectbox(
    "Select an Item-Store ID to Analyze",
    options=unique_ids,
    index=0 # Default to the first ID in the list
)


# --- Filter Data based on Selection ---
filtered_forecast = forecast_data[forecast_data['ID'] == selected_id]
filtered_inventory = inventory_data[inventory_data['ID'] == selected_id]


# --- Main Content ---

# Section 1: Demand Forecast Visualization
st.header(f"📈 Demand Forecast for `{selected_id}`")

if not filtered_forecast.empty:
    forecast_chart = alt.Chart(filtered_forecast).mark_line(
        point=True,
        tooltip=True,
        strokeWidth=3
    ).encode(
        x=alt.X('FULL_DATE:T', title='Forecast Date'),
        y=alt.Y('PREDICTED_SALES:Q', title='Predicted Unit Sales'),
        color=alt.value("#0068c9") # Streamlit blue
    ).properties(
        title=f"Predicted Sales for the Next 28 Days"
    ).interactive()

    st.altair_chart(forecast_chart, use_container_width=True)
else:
    st.warning("No forecast data available for the selected ID.")


# Section 2: Inventory Recommendations
st.header(f"📊 Inventory Recommendations for `{selected_id}`")

if not filtered_inventory.empty:
    # Use columns for a cleaner layout
    col1, col2 = st.columns(2)

    # Display Safety Stock
    safety_stock = filtered_inventory['SAFETY_STOCK'].iloc[0]
    col1.metric(
        label="Safety Stock (Units)",
        value=f"{safety_stock:,.0f}",
        help="The buffer stock kept to mitigate risk of stockouts caused by uncertainties in supply and demand."
    )

    # Display Reorder Point
    reorder_point = filtered_inventory['REORDER_POINT'].iloc[0]
    col2.metric(
        label="Reorder Point (Units)",
        value=f"{reorder_point:,.0f}",
        help="The inventory level at which a new order should be placed. It is the sum of demand during lead time and safety stock."
    )
    
    st.markdown("---")
    st.subheader("Inventory Data Details")
    st.dataframe(filtered_inventory)

else:
    st.warning("No inventory data available for the selected ID.")


# Section 3: Raw Data Explorer
with st.expander("Explore Raw Forecast Data"):
    st.dataframe(forecast_data)

st.sidebar.markdown("---")
st.sidebar.info("This dashboard is powered by dbt, Snowflake, Snowpark ML, and Streamlit.")