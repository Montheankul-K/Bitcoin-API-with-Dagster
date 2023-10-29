from dagster import MetadataValue, asset
import pandas as pd
import matplotlib.pyplot as plt
import requests
import base64
import io

# Create asset
@asset(
    # Define group name and compute kind
    group_name="bitcoin_price",
    compute_kind="python"
)

# Defind function to ingest data from coincap.io
def bitcoin_raw_json(context):
    """
        Get bitcoin price via coincap API
    """
    price_url = "https://api.coincap.io/v2/assets/bitcoin/history?interval=d1"
    price_data = requests.get(price_url).json()
    
    # Add output to Dagster
    context.add_output_metadata({'Number of records': len(price_data['data'])})
    return price_data

# Create asset
@asset(
    # Define group name and compute kind
    group_name="bitcoin_price",
    compute_kind="pandas"
)

# Defind function to transform data
def bitcoin_dataframe(bitcoin_raw_json):
    """
        Convert JSON Result to Pandas DataFrame
    """
    df = pd.DataFrame(bitcoin_raw_json['data'])
    df['date'] = pd.to_datetime(df['date'])
    df['priceUsd'] = df['priceUsd'].astype(float)

    return df

# Create asset
@asset(
    # Define group name and compute kind
    group_name="bitcoin_price",
    compute_kind="matplotlib"
)

# Define function to plot data
def bitcoin_plot(context, bitcoin_dataframe):
    """
        Plot price data graph
    """
    # Draw plot
    plt.figure(figsize=(10, 6))
    plt.plot(bitcoin_dataframe['date'], bitcoin_dataframe['priceUsd'])
    plt.title('R2AE - Bitcoin Price Over Time')
    plt.xlabel('Date')
    plt.ylabel('Price (USD)')
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save plot to buffer
    buffer = io.BytesIO()
    plt.savefig(buffer, format="png")

    # Get the content of the image and encode it to base64
    encoded_image = base64.b64encode(buffer.getvalue()).decode('utf-8')
    buffer.close()

    # Save base64 to metadata
    markdown_code = f"![Bitcoin Price Over Time](data:image/png;base64,{encoded_image})"
    
    # Save to markdown
    context.add_output_metadata({
        "preview": MetadataValue.md(markdown_code)
    })
