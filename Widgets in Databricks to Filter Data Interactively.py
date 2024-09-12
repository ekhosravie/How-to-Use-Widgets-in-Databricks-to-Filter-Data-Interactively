# Databricks notebook source
# Create a dropdown widget for region selection
dbutils.widgets.dropdown("region", "North America", ["North America", "Europe", "Asia", "Australia"], "Select Region")

# Create a text widget for sales threshold input
dbutils.widgets.text("sales_threshold", "10000", "Sales Threshold")

# Create a multiselect widget for selecting product categories
dbutils.widgets.multiselect("product_categories", "All", ["All", "Electronics", "Furniture", "Clothing", "Food"], "Select Categories")

# COMMAND ----------

# Access the widget values
selected_region = dbutils.widgets.get("region")
sales_threshold = float(dbutils.widgets.get("sales_threshold"))
selected_categories = dbutils.widgets.get("product_categories")

# COMMAND ----------

# Display the widget values for reference
print(f"Selected Region: {selected_region}")
print(f"Sales Threshold: {sales_threshold}")
print(f"Selected Categories: {selected_categories}")

# COMMAND ----------

# Sample DataFrame with sales data
sales_data = spark.createDataFrame([
    ("North America", "Electronics", 15000),
    ("Europe", "Furniture", 7000),
    ("Asia", "Clothing", 12000),
    ("Australia", "Food", 5000),
    ("North America", "Food", 20000)
], ["Region", "Category", "Sales"])

# Apply filters based on widget inputs
filtered_data = sales_data.filter((sales_data.Region == selected_region) & 
                                  (sales_data.Sales >= sales_threshold))

  # If specific categories are selected (not 'All'), filter the data further
if "All" not in selected_categories:
    filtered_data = filtered_data.filter(sales_data.Category.isin(selected_categories))

 # Show the filtered result
filtered_data.show()                                   


# COMMAND ----------


