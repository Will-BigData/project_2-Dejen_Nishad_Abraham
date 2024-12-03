# E-Commerce Analysis Project
## Prepared by: 

- **Dejen T**

- **Abraham O**

- **Nishad P**

# Executive Summary
This report provides an in-depth analysis of e-commerce sales patterns across different countries, product categories, cities, and peak sales times. By examining these factors, the analysis offers insights into consumer behavior that can inform strategic business decisions, optimize marketing efforts, and enhance the customer experience.

The data was analyzed using PySpark, focusing on four key questions:

1. **Top-Selling Categories by Country**: The analysis identified the most popular product categories in each country, revealing high demand for Living Room Furniture in the USA and Canada, Bedroom Furniture in the USA and Germany, and Home Office Furniture, particularly in the USA and Germany. These insights allow for targeted inventory management and marketing strategies to align with regional preferences.

2. **Product Popularity Throughout the Year by Country**: Seasonality and product demand were examined across multiple countries, showing peak sales in December across all regions, with additional spikes in summer months. This information helps the business anticipate demand fluctuations and plan for targeted marketing campaigns aligned with seasonal trends.

3. **Highest Sales Traffic Locations**: Key cities with the highest consumer demand were identified, including Perth in Australia, Calgary in Canada, Frankfurt in Germany, and Phoenix in the USA. By targeting these high-traffic locations with tailored promotions and efficient resource allocation, the business can maximize its reach and sales potential.

4. **Peak Sales Times by Country**: Analysis of peak sales times revealed country-specific trends, with Canada and Germany showing high activity during midday, while the USA exhibited late-night shopping patterns. These insights enable the business to optimize staffing, offer time-based promotions, and adjust inventory to meet consumer demand during peak hours.

In summary, this report highlights critical trends in e-commerce sales, including regional preferences, seasonal demand, high-traffic locations, and peak shopping hours. These findings equip the business with actionable insights to enhance customer engagement, drive sales, and refine marketing strategies across diverse markets.
# Introduction
- This analysis explores sales trends across countries, product categories, locations, and peak sales times to inform data-driven decisions. By identifying the top-selling items per country, seasonal product demand, high-traffic locations, and peak sales hours, we aim to support optimized marketing, inventory planning, and staffing. Using Spark for data processing and Power BI for visualization, we uncover insights that enhance strategic decision-making and improve overall sales performance.

# Objective
The objective of this analysis is to gain insights into sales patterns across different countries, product categories, locations, and times. By examining these factors, we aim to identify trends that can inform strategic business decisions, optimize marketing efforts, and enhance customer experience.

The analysis focuses on answering the following key questions:

1. **Top-Selling Category of Items Per Country**: Identify the most popular product categories in each country. This insight will allow the business to tailor inventory, promotions, and product availability according to regional preferences.
2. **Product Popularity Throughout the Year Per Country**: Understand how product demand fluctuates seasonally and across different regions. This will enable the business to anticipate demand shifts and plan targeted marketing campaigns accordingly.
3. **Locations with the Highest Sales Traffic**: Identify locations with the highest sales trafic to allocate resources effectively and target high-traffic areas with promotions or support initiatives.
4. **Peak Sales Times Per Country**: Determine the times of day with the highest sales activity in each country, allowing for planning, and targeted time-based promotions to maximize engagement.

# Analysis
### **1. What is the top selling category of items? Per country?**

 Identify the top-selling product categories across various countries based on total sales revenue.

- **Spark Output**:
    - The analysis reveals that **Living Room Furniture** is the top-selling category in the USA and Canada, with total sales of $907,375 and $775,788, respectively, indicating high demand in North American markets.
    - **Bedroom Furniture** also ranks prominently across the USA, Germany, and Canada, with sales reaching $760,739 in the USA and $686,811 in Germany, showing consistent consumer interest in bedroom furnishings.
    - **Home Office Furniture** sees notable sales in the USA ($553,472) and Germany ($463,816), aligning with a global trend towards remote work.

    - Spark Sample output:

    ![Top Selling Categories Per Country](/final_data/images/topSallingCountry.png)


    - POWER BI VISUALIZATION:

 ![Top Selling Categories Per Country](/final_data/images/Q1.png)
    

Example Code (Documented):

```python
# 1. Top-selling category of items per country
top_selling_category = df.withColumn("total_sale", col("qty") * col("price")) \\
    .groupBy("product_category", "country") \\
    .agg(sum("total_sale").alias("total_sales_amount")) \\
    .orderBy(col("total_sales_amount").desc()) \\
    .withColumn("analysis_type", lit("Top-selling category per country"))

# Display the top-selling categories per country
print("Top Selling Categories Per Country:")
top_selling_category.show()

```

### **2.  How does the popularity of products change throughout the year? Per country?**

The analysis of product popularity throughout the year reveals distinct seasonal trends across countries:

Spark output:

- **USA**: Product popularity peaks in **December** (2,015 units) and **July** (1,990 units), suggesting strong sales during holiday and mid-year periods.
- **Canada**: Product sales are highest in **December** (1,638 units), followed by steady increases in **summer months** (June and August), indicating a preference for seasonal buying during holidays and summer.
- **Germany**: Sales peak in **December** (1,902 units), with significant activity also in **March** and **August**, pointing to holiday and seasonal demand in early spring and late summer.
- **Australia**: The highest monthly sales occur in **December** (1,060 units) and **July** (971 units), with a noticeable increase during mid-year and holiday seasons, suggesting similar trends to the USA.

Overall, **December** consistently shows the highest product popularity across all countries, reflecting a global holiday-season surge. Other peaks vary by country, with summer and mid-year months also showing increased demand, particularly in North America and Australia. These trends highlight opportunities for targeted promotions and inventory adjustments aligned with peak demand periods in each region.

POWER BI VISUALIZATION:
 ![Top Selling Categories Per Country](/final_data/images/products.png)

exmple code:
```python
# Display a separate table for each country
for country in countries:
    print(f"Popularity of Products by Month in {country}:")
    popularity_by_month_country.filter(col("country") == country).show(truncate=False)
print("Popularity of Products by Month and Country:")
popularity_by_month_country.show(50, truncate=False)
```

### 3. Which locations see the highest traffic of sales?

The analysis identifies the locations with the highest sales traffic across multiple countries, highlighting cities with strong consumer demand.

- **Australia**: Perth and Melbourne lead in sales traffic, with Perth having the highest total sales (1,652), followed closely by Melbourne (1,100). This indicates strong purchasing activity in these cities, especially for specific high-demand categories.
- **Canada**: Calgary stands out with 1,645 sales, the highest traffic among Canadian cities, while Toronto (607) and Montreal (562) follow. This suggests Calgary as a key market in Canada with sustained consumer interest.
- **Germany**: Frankfurt shows the highest sales in Germany at 1,109, with other cities like Berlin (571) and Cologne (550) also contributing significantly to sales traffic, marking them as important retail hubs.
- **USA**: Sales traffic is most prominent in Phoenix (564) and Chicago (562), with Los Angeles following closely at 543. These cities exhibit high purchasing activity, suggesting potential for targeted marketing strategies.

In summary, **Perth (Australia)** and **Calgary (Canada)** are the top-performing cities with the highest sales traffic, followed by **Frankfurt (Germany)** and **Phoenix (USA)**. This data is crucial for guiding regional marketing efforts and inventory management to meet demand in these high-traffic locations.

POWER BI VISUALIZATION:
 ![Top Selling Categories Per Country](/final_data/images/SaleByCountry.png)


```python

# 3. Locations with the highest traffic of sales
highest_traffic_locations = df.groupBy("city", "country") \
    .agg(count("*").alias("total_sales")) \
    .orderBy(col("total_sales").desc()) \
    .withColumn("analysis_type", lit("Highest traffic locations"))
print("Locations with Highest Traffic of Sales:")
highest_traffic_locations.show(20, truncate=False)

```


### 4. What times have the highest traffic of sales? Per country?
This analysis identifies the peak sales hours for each country, providing insights into the times when consumer demand is highest. By understanding these patterns, the business can optimize staffing, marketing, and inventory management to better meet demand during peak hours.

- **Australia**: The highest sales traffic occurs at 13:00 with a sales count of 162, followed by 14:00 and 17:00, each with 153 sales. This indicates a strong consumer presence during mid-afternoon to early evening, making it an ideal time for targeted promotions and enhanced customer support.

- **Canada**: Sales traffic is highest at 15:00 with 154 sales, followed closely by 11:00 and 13:00, each with 153 sales. This suggests that midday to early afternoon is the most active period for sales in Canada, which could be leveraged for time-based offers or campaigns.

- **Germany**: The peak hour in Germany is 14:00 with 129 sales, with additional high traffic around 19:00 (126 sales) and 11:00 (128 sales). This trend indicates that afternoon hours are most popular, highlighting a key period for marketing and operational focus.

- **USA**: In the USA, the highest traffic occurs at 23:00 with 91 sales, followed by 0:00 with 82 sales. The data indicates a unique late-night shopping pattern, which may be due to night-shift workers or a general preference for late-night online shopping. This suggests a potential opportunity for late-night promotions or support services.

POWER BI VISUALIZATION:
 ![Top Selling Categories Per Country](/final_data/images/SalesTrafficPowerBI.png)

### Example code

```python

# 4. Times with the highest traffic of sales per country

traffic_by_hour_country = df.withColumn("hour", hour("datetime")) \
    .groupBy("country", "hour") \
    .agg(count("*").alias("sales_count")) \
    .orderBy(col("hour").asc()) \
    .withColumn("analysis_type", lit("Traffic by hour per country"))

for country in countries:
    print(f"Traffic of Sales by Hour in {country}:")
    traffic_by_hour_country.filter(col("country") == country).show(24, truncate=False)
print("Traffic of Sales by Hour Per Country:")
traffic_by_hour_country.show(96, truncate=False)
```

# Conclusion
In conclusion, this analysis provides valuable insights into e-commerce sales trends across countries, product categories, locations, and peak sales times. By examining these factors, businesses can make informed decisions to optimize marketing strategies, inventory management, and customer engagement. The key findings include:
 
- **Top-Selling Categories by Country**: Identified the most popular product categories in each country, enabling targeted marketing and inventory planning.
- **Product Popularity Throughout the Year by Country**: Revealed seasonal trends and peak sales months, guiding strategic planning and promotional campaigns.
- **Highest Sales Traffic Locations**: Identified key cities with high consumer demand, allowing for targeted marketing and resource allocation.
- **Peak Sales Times by Country**: Analyzed peak sales hours to optimize staffing, promotions, and inventory management during high-traffic periods.

By leveraging these insights, businesses can enhance their competitive edge, drive sales growth, and improve customer satisfaction. The data-driven approach presented in this report equips organizations with actionable information to make informed decisions and capitalize on emerging opportunities in the dynamic e-commerce landscape.