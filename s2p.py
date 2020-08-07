import pandas as pd
import geopandas as gpd
from sqlalchemy import *
from geoalchemy2 import Geometry, WKTElement

def ALLCAPS(df):
    df.columns = map(str.upper, df.columns)
    df.columns = df.columns.str.replace(' ','_')
    return df

#Create engine for database
engine = create_engine('postgresql://postgres:password@localhost:5432/to_sql')

#Import and read csv file. Could be replaced with SODA API
csv = 'C:/Users/test/Documents/Food_Establishment_Inspection_Scores.csv'
sc = pd.read_csv(csv)


#DATA CLEANING
#Split the address into new columns where there is a newline
sc[['STREET','CITY','latlng']]=sc.Address.str.split("\n",expand=True)
#Get ZIP from CITY
sc['ZIP'] = sc.CITY.str[-6:]
sc['CITY'] = sc.CITY.str[:-6]
#Strip the end characters on the latlong column.
sc.latlng = sc.latlng.str.strip("()")
#Split latlng to lat, long columns at ','.
sc[['LAT','LNG']]=sc.latlng.str.split(",",expand=True)
#Strip whitespace.
sc.LAT = sc.LAT.str.strip()
sc.LNG = sc.LNG.str.strip()
#Convert dtype from object to float64/int64
sc[["LAT", "LNG"]] = sc[["LAT", "LNG"]].apply(pd.to_numeric)
#Drop latlng column
sc = sc.drop(['latlng','Address','Zip Code'], axis=1)

#Call a function that all caps the column names and replaces whitespace.
#Useful if we are putting them into a database
sc = ALLCAPS(sc)
sc['INSPECTION_DATE'] = pd.to_datetime(sc.INSPECTION_DATE)

#DATAFRAME TO GEODATAFRAME
gdf = gpd.GeoDataFrame(
    sc, geometry=gpd.points_from_xy(x=sc.LNG, y=sc.LAT)
)
#GEODATAFRAME TO POSTGRES

gdf['geom'] = gdf['geometry'].apply(lambda x: WKTElement(x.wkt, srid=6343))

#Drop the geometry column as it is now duplicative
#Drop empyt rows for now!!!!
gdf = gdf.drop(columns=['geometry'])
gdf.dropna(subset=['LAT'], inplace=True)


tablename = 'austin_food_insp'
# Use 'dtype' to specify column's type
"""'RESTAURANT_NAME', 'INSPECTION_DATE', 'SCORE', 'FACILITY_ID',
    'PROCESS_DESCRIPTION', 'STREET', 'CITY', 'ZIP', 'LAT', 'LNG', 'geom'
# For the geom column, we will use GeoAlchemy's type 'Geometry'"""
gdf.to_sql(
    tablename,
    con=engine,
    if_exists='replace',
    index=False,
    dtype={
        'RESTURANT_NAME': String(),
        'INSPECTION_DATE': DateTime(timezone=False),
        'SCORE': Integer(),
        'FACILITY_ID': String(),
        'PROCESS_DESCRIPTION': String(),
        'STREET': String(),
        'CITY': String(),
        'ZIP': String(),
        'LAT': Float(),
        'LNG': Float(),
        'geom': Geometry('POINT', srid=6343)
    }
)
