import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
#-------------------------------
# PRIMER PUNTO 
# Database connection settings (match environment values in docker-compose.yml)
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="bigdatatools1",
    user="psqluser",
   password="psqlpass"
)

cur = conn.cursor()
cur.execute("SELECT version();")
ver = cur.fetchone()
print("PostgreSQL database version:", ver[0])

cur.close()
conn.close()
try:
    # Intentar conexión
    conn = psycopg2.connect(
        host="localhost",
        port=5433,  # Asegúrate que es el mismo que expusiste en docker-compose
        database="bigdatatools1",
        user="psqluser",
        password="psqlpass"
    )
    cur = conn.cursor()
    
    # Ejecutar prueba
    cur.execute("SELECT version();")
    ver = cur.fetchone()
    print("✅ Connection successful!")
    print("PostgreSQL database version:", ver[0])

except psycopg2.OperationalError as e:
    print("❌ Error: Could not connect to PostgreSQL.")
    print(e)

except Exception as e:
    print("❌ Unexpected error:", e)

finally:
    # Cerrar conexión si existe
    try:
        if cur:
            cur.close()
        if conn:
            conn.close()
        print("🔒 Connection closed.")
    except NameError:
        # Si nunca se creó conn/cur
        print("No connection was established.")
#-------------------------------
population=pd.read_parquet('World-Population-Estimates.parquet')
population.info()
population.drop(columns=['Unnamed: 95'], inplace=True)
population.describe()
print(f"Indicators: {population['Indicator Name'].iloc[0]}")
relevant_indicators=['Life expectancy at birth, total (years)', 'Population, total', 'Fertility rate, total (births per woman)', 'Birth rate, crude (per 1,000 people)' , 'Mortality rate, neonatal (per 1,000 live births)' ]
population_data=population[population['Indicator Name'].isin(relevant_indicators)]
ind_cols=[col for col in population_data.columns if not col.isdigit()]
year_cols=[col for col in population_data.columns if col.isdigit() and 1991 <= int(col) <=2024]

population_data=population_data[ind_cols+year_cols]
print(f"Indicators: {population_data['Indicator Name'].unique()}")
population_data.info()
population_data
gdp=pd.read_csv('GDP.PCAP.PP.CD_DS2_en_csv_v2_37774.csv', sep=';')
gdp.info()
ind_cols=[col for col in gdp.columns if not col.isdigit()]
year_cols=[col for col in gdp.columns if col.isdigit() and 1991 <= int(col) <=2024]

gdp_data=gdp[ind_cols+year_cols]
gdp_data.info()
gdp_data
import io

id_vars = ['Country Name', 'Country Code', 'Indicator Name', 'Indicator Code']
# 1. Reshape population_data
pop_long = population_data.melt(
    id_vars=id_vars,
    var_name='Year',
    value_name='Value'
)

# 2. Reshape gdp_data
gdp_long = gdp_data.melt(
    id_vars=id_vars,
    var_name='Year',
    value_name='Value'
)

# 3. Combine the two long DataFrames
combined_data = pd.concat([pop_long, gdp_long], ignore_index=True)

# 4. Clean the data for database insertion
combined_data.dropna(subset=['Value'], inplace=True)  # Drop rows where the value is missing
combined_data['Year'] = combined_data['Year'].astype(int)  # Convert Year column to integer

# Rename columns for SQL friendliness (lowercase, no spaces)
combined_data.rename(columns={
    'Country Name': 'country_name', 'Country Code': 'country_code',
    'Indicator Name': 'indicator_name', 'Indicator Code': 'indicator_code',
    'Year': 'year', 'Value': 'value'
}, inplace=True)

print("Reshaped and combined data preview:")
print(combined_data.head())
print(f"\nTotal rows to insert: {len(combined_data)}")

# 5. Batch load into PostgreSQL using COPY command for high performance
table_name = 'country_indicators'

# Reconnect to the database
conn = psycopg2.connect(host="localhost", port=5433, database="bigdatatools1", user="psqluser", password="psqlpass")
cur = conn.cursor()

# Create table schema. Using TEXT is flexible, NUMERIC is good for values.
cur.execute(f"""
DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name} (
    id SERIAL PRIMARY KEY, country_name TEXT, country_code VARCHAR(3),
    indicator_name TEXT, indicator_code TEXT, year INTEGER, value NUMERIC
);""")
conn.commit()
print(f"Table '{table_name}' created successfully.")

# Use an in-memory buffer (StringIO) to prepare data for COPY
buffer = io.StringIO()
combined_data[['country_name', 'country_code', 'indicator_name', 'indicator_code', 'year', 'value']].to_csv(buffer, header=False, index=False)
buffer.seek(0)  # Rewinds the buffer to the beginning

# Use copy_expert to load data using the CSV format, which correctly handles quoted values.
# This is more robust than copy_from when data fields might contain the separator character (comma).
try:
    # The SQL command specifies the columns and that the format is CSV, which pandas.to_csv() produces.
    sql_copy_command = f"""
        COPY {table_name} (country_name, country_code, indicator_name, indicator_code, year, value)
        FROM STDIN WITH (FORMAT CSV, HEADER FALSE)
    """
    cur.copy_expert(sql=sql_copy_command, file=buffer)
    conn.commit()
    print("Data loaded successfully using COPY.")
except (Exception, psycopg2.DatabaseError) as error:
    print(f"Error: {error}")
    conn.rollback()
finally:
    cur.execute(f"SELECT COUNT(*) FROM {table_name};")  # Verify the load
    print(f"Verification: {cur.fetchone()[0]} rows were inserted into '{table_name}'.")
    cur.close()
    conn.close()
#-------------------------------------------
#PUNTO B 
import io, time, psycopg2

table_copy = "country_indicators_copy"
table_insert = "country_indicators_insert"

def create_table(cur, table_name):
    cur.execute(f"""
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY, 
            country_name TEXT, 
            country_code VARCHAR(3),
            indicator_name TEXT, 
            indicator_code TEXT, 
            year INTEGER, 
            value NUMERIC
        );
    """)

# ====================
# MÉTODO 1: COPY
# ====================
conn = psycopg2.connect(host="localhost", port=5433, database="bigdatatools1", user="psqluser", password="psqlpass")
cur = conn.cursor()

create_table(cur, table_copy)
conn.commit()

buffer = io.StringIO()
combined_data[['country_name','country_code','indicator_name','indicator_code','year','value']].to_csv(buffer, header=False, index=False)
buffer.seek(0)

start = time.time()
cur.copy_expert(f"""
    COPY {table_copy} (country_name, country_code, indicator_name, indicator_code, year, value)
    FROM STDIN WITH (FORMAT CSV)
""", buffer)
conn.commit()
end = time.time()

print(f"⏱ COPY execution time: {end - start:.2f} seconds")

cur.execute(f"SELECT COUNT(*) FROM {table_copy};")
print("Rows loaded with COPY:", cur.fetchone()[0])

cur.close()
conn.close()


# ====================
# MÉTODO 2: INSERT fila por fila
# ====================
conn = psycopg2.connect(host="localhost", port=5433, database="bigdatatools1", user="psqluser", password="psqlpass")
cur = conn.cursor()

create_table(cur, table_insert)
conn.commit()

start = time.time()
for _, row in combined_data.iterrows():
    cur.execute(f"""
        INSERT INTO {table_insert} (country_name, country_code, indicator_name, indicator_code, year, value)
        VALUES (%s, %s, %s, %s, %s, %s);
    """, (
        row['country_name'],
        row['country_code'],
        row['indicator_name'],
        row['indicator_code'],
        int(row['year']),
        float(row['value']) if pd.notna(row['value']) else None
    ))
conn.commit()
end = time.time()

print(f"⏱ INSERT execution time: {end - start:.2f} seconds")

cur.execute(f"SELECT COUNT(*) FROM {table_insert};")
print("Rows loaded with INSERT:", cur.fetchone()[0])

cur.close()
conn.close()
#--------------------------------------------------
# Ensure the SQLAlchemy engine is available
db_url = "postgresql+psycopg2://psqluser:psqlpass@localhost:5433/bigdatatools1"
engine = create_engine(db_url)

# Define the list of countries/regions to compare
countries_to_compare = ('United States', 'Japan', 'China', 'Germany', 'United Kingdom', 'European Union')


query = """
        SELECT
            year,
            country_name,
            (MAX(CASE WHEN indicator_name = 'GDP per capita PPP (current international $)' THEN value END) *
             MAX(CASE WHEN indicator_name = 'Population, total' THEN value END)) AS total_gdp_ppp
        FROM
            country_indicators
        WHERE
            country_name IN %(countries)s
          AND indicator_name IN ('GDP per capita PPP (current international $)', 'Population, total')
        GROUP BY
            country_name, year
        ORDER BY
            country_name, year;
        """

# Execute the query using pandas and SQLAlchemy
gdp_comparison_df = pd.read_sql_query(query, engine, params={'countries': countries_to_compare})

print("Total GDP (PPP) Data for Major Economies:")
print(gdp_comparison_df.head())

# Visualize the comparison using a line plot with Matplotlib
fig, ax = plt.subplots(figsize=(14, 8))

# To plot multiple lines from a long-format DataFrame, we loop through each country
# and plot its data on the same axes.
for country in countries_to_compare:
    country_df = gdp_comparison_df[gdp_comparison_df['country_name'] == country]
    ax.plot(country_df['year'], country_df['total_gdp_ppp'], marker='o', linestyle='-', label=country)

ax.set_title('Total GDP (PPP) Evolution: USA, Japan, China, Germany, UK & EU', fontsize=18)
ax.set_xlabel('Year', fontsize=14)
ax.set_ylabel('Total GDP (PPP, current international $)', fontsize=14)
ax.legend(title='Country/Region')
ax.grid(True)
plt.show()
# To use SQLAlchemy, we first create an engine that manages connections to the database.
# This is a more robust way to interact with databases in Python applications.
db_url = "postgresql+psycopg2://psqluser:psqlpass@localhost:5433/bigdatatools1"
engine = create_engine(db_url)


query = """
        SELECT
            year, MAX (CASE WHEN indicator_name = 'GDP per capita PPP (current international $)' THEN value END) AS gdp_per_capita, MAX (CASE WHEN indicator_name = 'Life expectancy at birth, total (years)' THEN value END) AS life_expectancy
        FROM
            country_indicators
        WHERE
            country_name = 'United States'
          AND indicator_name IN (
            'GDP per capita PPP (current international $)'
            , 'Life expectancy at birth, total (years)'
            )
        GROUP BY
            year
        ORDER BY
            year; \
        """

# Execute the query and load the results into a pandas DataFrame
us_evolution_df = pd.read_sql_query(query, engine)

print("Data for US GDP vs. Life Expectancy:")
print(us_evolution_df.head())
# To compare the time series for GDP and Life Expectancy, we'll use a plot with two y-axes.
# This allows us to see both trends on the same chart, even though their scales are very different.
fig, ax1 = plt.subplots(figsize=(12, 7))

ax1.set_title('Time Series of GDP and Life Expectancy in the United States', fontsize=16)
ax1.set_xlabel('Year', fontsize=12)

# Configure the primary (left) y-axis for GDP
color1 = 'tab:blue'
ax1.set_ylabel('GDP per Capita (current international $)', color=color1, fontsize=12)
ax1.plot(us_evolution_df['year'], us_evolution_df['gdp_per_capita'], color=color1, marker='o', label='GDP per Capita')
ax1.tick_params(axis='y', labelcolor=color1)

# Create a secondary (right) y-axis that shares the same x-axis
ax2 = ax1.twinx()
color2 = 'tab:red'
ax2.set_ylabel('Life Expectancy at Birth (years)', color=color2, fontsize=12)
ax2.plot(us_evolution_df['year'], us_evolution_df['life_expectancy'], color=color2, marker='x', label='Life Expectancy')
ax2.tick_params(axis='y', labelcolor=color2)

# Add a unified legend
lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax2.legend(lines1 + lines2, labels1 + labels2, loc='upper left')

fig.tight_layout() # Adjust plot to prevent labels from overlapping
plt.show()
cont_blocks_df = pd.read_csv('country_classification.csv', sep=';')
cont_blocks_df.rename(columns={
    'Country Code': 'country_code',
    'bloc': 'economic_bloc' # Renaming for clarity and to avoid potential reserved words
}, inplace=True)

# Use the SQLAlchemy engine to load the DataFrame into the database
db_url = "postgresql+psycopg2://psqluser:psqlpass@localhost:5433/bigdatatools1"
engine = create_engine(db_url)
table_name = 'country_classification'
cont_blocks_df.to_sql(table_name, engine, if_exists='replace', index=False)

print(f"Table '{table_name}' created and loaded with {len(cont_blocks_df)} rows.")
#--------------------------------------------------
# PUNTO DEL CUADERNILLO 
from matplotlib.ticker import FuncFormatter

# Ensure the SQLAlchemy engine is available
db_url = "postgresql+psycopg2://psqluser:psqlpass@localhost:5433/bigdatatools1"
engine = create_engine(db_url)

# The query first calculates the total GDP for each BRICS country, then sums them up by year.
# It then calculates the total GDP for the US.
# Finally, it combines these two results using UNION ALL.
query = """
        WITH brics_country_gdp AS (
            -- Step 1: Calculate total GDP for each individual BRICS country using a JOIN
            SELECT ci.year,
                   ci.country_name,
                   (MAX(CASE
                            WHEN ci.indicator_name = 'GDP per capita PPP (current international $)' THEN ci.value END) *
                    MAX(CASE WHEN ci.indicator_name = 'Population, total' THEN ci.value END)) AS total_gdp
            FROM country_indicators ci
                     JOIN country_classification cc ON ci.country_code = cc.country_code
            WHERE cc.economic_bloc = 'BRICS'
              AND ci.indicator_name IN ('GDP per capita PPP (current international $)', 'Population, total')
            GROUP BY ci.year, ci.country_name
            -- Ensure both population and gdp_per_capita exist for the calculation
            HAVING MAX(CASE
                           WHEN ci.indicator_name = 'GDP per capita PPP (current international $)'
                               THEN ci.value END) IS NOT NULL
               AND MAX(CASE WHEN ci.indicator_name = 'Population, total' THEN ci.value END) IS NOT NULL),
             brics_total_gdp AS (
                 -- Step 2: Sum the GDP of all EU countries for each year
                 SELECT
            year, 'BRICS (Calculated)' AS entity, SUM (total_gdp) AS total_gdp_ppp
        FROM brics_country_gdp
        GROUP BY year
            ),
            us_total_gdp AS (
        -- Step 3: Calculate total GDP for the United States
        SELECT
            year, 'United States' AS entity, (MAX (CASE WHEN indicator_name = 'GDP per capita PPP (current international $)' THEN value END) *
            MAX (CASE WHEN indicator_name = 'Population, total' THEN value END)) AS total_gdp_ppp
        FROM country_indicators
        WHERE
            country_name = 'United States'
          AND indicator_name IN ('GDP per capita PPP (current international $)'
            , 'Population, total')
        GROUP BY year
            )
-- Step 4: Combine the results
        SELECT *
        FROM brics_total_gdp
        UNION ALL
        SELECT *
        FROM us_total_gdp
        ORDER BY entity, year; \
        """

# Execute the query
us_brics_gdp_df = pd.read_sql_query(query, engine)

# For a bar plot, it's best to compare a few specific years
years_for_plot = [1995, 2005, 2015, 2021]  # Choosing some representative years
plot_df = us_brics_gdp_df[us_brics_gdp_df['year'].isin(years_for_plot)]

# Pivot the data to make it suitable for a grouped bar plot and use pandas plotting
pivot_df = plot_df.pivot(index='year', columns='entity', values='total_gdp_ppp')
ax = pivot_df.plot(kind='bar', figsize=(12, 8), width=0.8, colormap='viridis')

ax.set_title('GDP Comparison: United States vs. BRICS (Calculated)', fontsize=18)
ax.set_xlabel('Year', fontsize=14)
ax.set_ylabel('Total GDP (PPP, current international $)', fontsize=14)
ax.tick_params(axis='x', rotation=0)  # Keep year labels horizontal


# Format y-axis to be more readable (in trillions)
def trillions(x, pos):
    'The two args are the value and tick position'
    return f'${x * 1e-12:1.1f}T'

formatter = FuncFormatter(trillions)
ax.yaxis.set_major_formatter(formatter)

ax.legend(title='Entity')
ax.grid(True, axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()
#----------------------------------
#PUNTO C 
# ASEAN countries
asean = [
    "Brunei Darussalam", "Cambodia", "Indonesia", "Lao PDR",
    "Malaysia", "Myanmar", "Philippines", "Singapore",
    "Thailand", "Vietnam"
]

# European Union countries (2025 membership list, 27 members)
eu = [
    "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czech Republic",
    "Denmark", "Estonia", "Finland", "France", "Germany", "Greece", "Hungary",
    "Ireland", "Italy", "Latvia", "Lithuania", "Luxembourg", "Malta",
    "Netherlands", "Poland", "Portugal", "Romania", "Slovak Republic",
    "Slovenia", "Spain", "Sweden"
]

usmca = ["United States", "Mexico", "Canada"]

#Filtrar y agregar datos
# Agregar columna 'region_group'
def classify_region(country):
    if country in asean:
        return "ASEAN"
    elif country in eu:
        return "European Union"
    elif country in usmca:
        return "USMCA"
    else:
        return "Other"

combined_data["region_group"] = combined_data["country_name"].apply(classify_region)

# Calcular métricas para ASEAN, EU y USMCA
region_summary = (
    combined_data[combined_data["region_group"].isin(["ASEAN", "European Union", "USMCA"])]
    .groupby(["region_group", "year", "indicator_name"])["value"]
    .sum()
    .reset_index()
)

print(region_summary.head())

# Confirmar qué indicadores hay para población
print(region_summary["indicator_name"].unique()[:20])  

# Filtrar indicador de población (ajusta el nombre exacto que aparezca en el print de arriba)
pop_plot = region_summary[region_summary["indicator_name"].str.contains("Population, total", case=False, na=False)]

# Visualización
plt.figure(figsize=(10,6))
for region in ["ASEAN", "European Union", "USMCA"]:
    subset = pop_plot[pop_plot["region_group"] == region]
    if not subset.empty:
        plt.plot(subset["year"], subset["value"], label=region)

plt.title("Population: ASEAN vs EU vs USMCA")
plt.xlabel("Year")
plt.ylabel("Population")
plt.legend()
plt.show()
#---------------------------------------------------
# PUNTO 2
import pandas as pd
from sqlalchemy import create_engine

# Crear el engine con SQLAlchemy
engine = create_engine("postgresql+psycopg2://psqluser:psqlpass@localhost:5433/bigdatatools1")


# ========================
# Consulta para ASEAN
# ========================
sql_asean = """
SELECT 
    life.country_name,
    life.value AS life_expectancy,
    gdp.value AS gdp_per_capita
FROM country_indicators life
JOIN country_indicators gdp
    ON life.country_name = gdp.country_name
   AND life.year = gdp.year
WHERE life.indicator_name = 'Life expectancy at birth, total (years)'
  AND gdp.indicator_name = 'GDP per capita PPP (current international $)'
  AND life.year = 2019
  AND life.country_name IN ('Brunei Darussalam', 'Cambodia', 'Indonesia', 'Lao PDR',
        'Malaysia', 'Myanmar', 'Philippines', 'Singapore',
        'Thailand', 'Vietnam')
  AND life.value > 75
  AND gdp.value > 20000;

"""


asean_results = pd.read_sql(sql_asean, engine)
print("ASEAN Results (2019):")
display(asean_results)


# ========================
# Consulta para MERCOSUR
# ========================
sql_mercosur = """
SELECT 
    life.country_name,
    life.value AS life_expectancy,
    gdp.value AS gdp_per_capita
FROM country_indicators life
JOIN country_indicators gdp
    ON life.country_name = gdp.country_name
   AND life.year = gdp.year
WHERE life.indicator_name = 'Life expectancy at birth, total (years)'
  AND gdp.indicator_name = 'GDP per capita PPP (current international $)'
  AND life.year = 2019
  AND life.country_name IN ('Argentina','Brazil','Paraguay','Uruguay','Venezuela')
  AND life.value > 75
  AND gdp.value > 20000;
"""



mercosur_results = pd.read_sql(sql_mercosur, engine)
print("MERCOSUR Results (2019):")
display(mercosur_results)
