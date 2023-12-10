import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

df = pd.read_csv('Data/downloads/Validated_Airline_Dataset.csv')


# Analysis examples

# Basic statistics
summary_stats = df.describe()

# Count of flights by country
flight_count_by_country = df['Country Name'].value_counts()

# Count of flights by flight status
flight_count_by_status = df['Flight Status'].value_counts()

# Plotting
top_countries = df['Country Name'].value_counts().nlargest(50).index

# Filter the DataFrame for the top 100 countries
df_top_countries = df[df['Country Name'].isin(top_countries)]
#
# Create the countplot for the top 100 countries
plt.figure(figsize=(12, 6))
sns.countplot(x='Country Name', data=df_top_countries)
plt.title('Flight Count by Country (Top 50)')
plt.xlabel('Country')
plt.ylabel('Flight Count')
plt.xticks(rotation=90)  # Rotate x-axis labels for better readability
plt.show()


# Pie chart for flight count by status
plt.figure(figsize=(8, 8))
plt.pie(flight_count_by_status, labels=flight_count_by_status.index, autopct='%1.1f%%', startangle=90)
plt.title('Flight Count by Status')
plt.show()

top_airlines = df['Arrival Airport'].value_counts().nlargest(10)  # Adjust the number as needed


plt.figure(figsize=(12, 6))
sns.barplot(x=top_airlines.index, y=top_airlines.values)
plt.title('Top Airports with the Highest Number of Incoming Flights')
plt.xlabel('Airline')
plt.ylabel('Flight Count')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.show()


top_n = 25
top_airports = df['Arrival Airport'].value_counts().nlargest(top_n).index

# Filter the DataFrame to include only the top N Arrival Airports
df_top_n = df[df['Arrival Airport'].isin(top_airports)]

plt.figure(figsize=(14, 8))
sns.countplot(x='Arrival Airport', hue='Flight Status', data=df_top_n, order=top_airports)
plt.title(f'Top {top_n} Arrival Airports - Flight Count by Status')
plt.xlabel('Arrival Airport')
plt.ylabel('Flight Count')
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
plt.legend(title='Flight Status')
plt.show()