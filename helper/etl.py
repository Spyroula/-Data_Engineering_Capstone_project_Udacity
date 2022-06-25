import os, re
import configparser
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, lower, isnull, year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, to_date
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType
from pyspark.sql.types import *

# The configuration file "dl.cfg" includs the AWS key id and secret access key
config = configparser.ConfigParser()
config.read('./helper/dl.cfg')
#The AWS access key information are saved in  environment variables

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["PATH"] = "/opt/conda/bin:/opt/spark-2.4.3-bin-hadoop2.7/bin:/opt/conda/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/lib/jvm/java-8-openjdk-amd64/bin"
os.environ["SPARK_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "/opt/spark-2.4.3-bin-hadoop2.7"


def create_spark_session():
    spark = SparkSession.builder.\
    config("spark.jars.repositories", "https://repos.spark-packages.org/").\
    config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3").\
    config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
    enableHiveSupport().getOrCreate()
    
    return spark 

    
def load_data(spark, input_path, input_format = "csv", columns = '*', debug_size = None, **options):
    """
    Loads data from a given input path using the pyspark module and returns the data
    in a spark 'DataFrame' format.
    
    Patameters
    --------------
    spark : obj
        The Spark session, representing the entry point to programming Spark.
    input_path : str
        Directory where the input files are located.
    input_format : str
        The format of the data source files. Default value: 'csv'.
    columns : list 
        List of columns names of the returned Spark dataframe. 
        Default value: "*" (e.g. all columns).
    debug_size : int 
        The number of rows to returned for debug purposes. 
        Default value: None (e.g. all rows).
    options: str
        All the other string options.
    
    Returns
    --------------
    df : DataFrame
        The spark 'DataFrame'
        
    """   
    
    if debug_size is None:
        df = spark.read.load(input_path, format=input_format, **options).select(columns)
    else:
        df = spark.read.load(input_path, format=input_format, **options).select(columns).limit(debug_size)
    
    return df

  

def save_data(df, output_path, mode = "overwrite", output_format = "parquet", columns = '*', partitionBy=None, **options):
    """
    Saves the DataFrame to a given output destination. The output format and a set of options are specified.
    
    
    Patameters
    --------------
    df : DataFrame
        The Spark DataFrame.
    output_path : str
        The output path were the DataFrame will be stored. The path is located in
        a Hadoop supported file system.
    mode : str
        The saving mode when the data already exists. Default value: 'overwrite'.
    output_format : str
        The format of the data we wish to save at the output path. Default value: 'parquet'.
    columns : list
        List of columns names of the returned Spark dataframe. 
        Default value: "*" (e.g. all columns).
    partitionBy : list (
        List of partitioning columns names. Default value: None (e.g no partitions).
    options : str
        All the other available string options.
    """

    df.select(columns).write.save(output_path, mode= mode, format=output_format, partitionBy = partitionBy, **options)
    
def process_immigration_data(spark, input_path, output_path,
                         date_output_path, columns, partitionBy,
                         input_format = "csv", 
                         debug_size = None, saved_columns='*', header=True, **options):
    """
    Loads the the immigration dataset stored in the given input_path, 
    performs the ETL processes and saves the resulted data in the  given output path.
    
    Patameters
    --------------
    spark : obj
        The Spark session, representing the entry point to programming Spark.
    input_path : str
        Directory where the input files are located.
    output_path : str
        The output path were the immigration output files will be stored. 
    date_output_path : str  
        The output path were the date output files will be stored.
    columns : list 
        List of columns names to read in.
    input_format : str
        The format of the input data files. Default value: 'csv'.
    partitionBy : list (
        List of partitioning columns names. 
    debug_size : int
        The number of rows to returned for debug purposes. 
        Default value: None (e.g. all rows). 
    saved_columns : list
        List of columns that will be saved.
        Default value: "*" (e.g. all columns).
    header : bool
        Header will be used as names of columns. Default value: False.
    options : str
        All the other available string options.
        
    Returns
    --------------
    immigration : DataFrame
        The immigration DataFrame
    """
    
    # Loads the Spark immigration dataframe 
    immigration = load_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, debug_size = debug_size, header=header, **options)
   
    ## Preprocessing steps
    # Convert the string/double type columns into integer.
    integer_columns = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 
        'arrdate', 'i94mode', 'i94bir', 'i94visa', 'count', 'biryear', 'dtadfile', 'depdate']
    immigration = convert_type(immigration, dict(zip(integer_columns, len(integer_columns)*[IntegerType()])))
    
    # Convert SAS datatype date to a meaningful date in string format of YYYY-MM-DD
    date_columns = ['arrdate', 'depdate']
    immigration = convert_SAS_date(immigration, date_columns)
    
    # Drop columns with high null entries and/or not useful columns
    high_null = ["visapost", "occup", "entdepu", "insnum"]
    unuseful_cols = ["count", "entdepa", "entdepd", "matflag", "dtaddto", "biryear", "asdmnum"]
    immigration = immigration.drop(*high_null)
    immigration = immigration.drop(*unuseful_cols)
    
    # Generate the `visitor stay lenght` in the US columns 
    immigration = immigration.withColumn('stay', date_difference_udf(immigration.arrdate, immigration.depdate))
    immigration = convert_type(immigration, {'stay': IntegerType()})
    
    # Create the DATE DataFrame and save it to the output_path indicated as parameter of the function
    if date_output_path is not None:
        arrdate = immigration.select('arrdate').distinct()
        depdate = immigration.select('depdate').distinct()
        dates = arrdate.union(depdate)
        dates = dates.withColumn("date", to_date(dates.arrdate, date_format))
        dates = dates.withColumn("year", year(dates.date))
        dates = dates.withColumn("month", month(dates.date))
        dates = dates.withColumn("day", dayofmonth(dates.date))
        dates = dates.withColumn("weekofyear", weekofyear(dates.date))
        dates = dates.withColumn("dayofweek", dayofweek(dates.date))
        dates = dates.drop("date").withColumnRenamed('arrdate', 'date')
        dates = dates.filter(dates.date. isNotNull())
        save_data(df=dates.select("date", "year", "month", "day", "weekofyear", "dayofweek"), output_path=date_output_path)
    
    # Save the processed immigration DataFrame to the output_path
    if output_path is not None:
        save_data(df=immigration.select(saved_columns), output_path=output_path, partitionBy = partitionBy)
    
    return immigration

def process_temperature_data(spark, input_path, output_path, partitionBy,
                         input_format = "csv", columns = '*', debug_size = None,  header=True, **options):
    """
    Loads the the global temperature dataset stored in the given input_path, 
    performs the ETL processes and saves the resulted data in the  given output path.
    
    Patameters
    --------------
    spark : obj
        The Spark session, representing the entry point to programming Spark.
    input_path : str
        Directory where the input files are located.
    output_path : str
        The output path were the temperature output files will be stored. 
    partitionBy : list (
        List of partitioning columns names.
    input_format : str
        The format of the input data files. Default value: 'csv'.
    columns : list 
        List of columns names to read in. (Note: Only some columns are useful.)
    debug_size : int
        The number of rows to returned for debug purposes. 
        Default value: None (e.g. all rows).
    header : bool
        Header will be used as names of columns. Default value: False.
    options : str
        All the other available string options.
         
    Returns
    --------------
    temperature : DataFrame
        The temperature DataFrame
    """
    # Loads the Spark global temperature dataframe 

    temperature = load_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, debug_size = debug_size, header=header, **options)
    # Save the processed temperature DataFrame to the output_path
    save_data(df=temperature, output_path=output_path, partitionBy = partitionBy)
    
    return temperature

def process_airport_data(spark, input_path, output_path, partitionBy,
                         input_format = "csv", columns = '*', debug_size = None,  header=True, **options):
    """
    Loads the the airport dataset stored in the given input_path, 
    performs the ETL processes and saves the resulted data in the  given output path.
    
    Patameters
    --------------
    spark : obj
        The Spark session, representing the entry point to programming Spark.
    input_path : str
        Directory where the input files are located.
    output_path : str
        The output path were the airport output files will be stored. 
    partitionBy : list (
        List of partitioning columns names. 
    input_format : str
        The format of the input data files. Default value: 'csv'.
    columns : list 
        List of columns names to read in. (Note: Only some columns are useful.)
    debug_size : int
        The number of rows to returned for debug purposes. 
        Default value: None (e.g. all rows).
    header : bool
        Header will be used as names of columns. Default value: False.
    options : str
        All the other available string options.
    
    Returns
    --------------
    airport : DataFrame
        The airport DataFrame
    """
    # Loads the Spark airport dataframe 
    airport = load_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, debug_size = debug_size, header=header, **options)    
    # Save the processed airport DataFrame to the output_path
    save_data(df=airport, output_path=output_path, partitionBy = partitionBy)
    
    return airport

def process_demographics_data(spark, input_path, output_path, partitionBy,
                         input_format = "csv", columns='*',
                          debug_size = None, header=True, sep=";", **options):
    """
    Loads the the demographics dataset stored in the given input_path, 
    performs the ETL processes and saves the resulted data in the  given output path.
    
    Patameters
    --------------
    spark : obj
        The Spark session, representing the entry point to programming Spark.
    input_path : str
        Directory where the input files are located.
    output_path : str
        The output path were the demographics output files will be stored.
    partitionBy : list (
        List of partitioning columns names.
    input_format : str
        The format of the input data files. Default value: 'csv'.
    columns : list 
        List of columns names to read in. (Note: Only some columns are useful.)
    debug_size : int
        The number of rows to returned for debug purposes. 
        Default value: None (e.g. all rows).
    header : bool
        Header will be used as names of columns. Default value: False.
    options : str
        All the other available string options.
    
    Returns
    --------------
    demographics : DataFrame
        The demographics DataFrame
    """
    # Loads the Spark demographics dataframe
    demographics = load_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, debug_size = debug_size, header=header, sep=sep, **options)
    
    # Convert numeric columns to the proper Integer and Double types
    integer_columns = ['Count', 'Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 'Foreign-born']
    float_columns = ['Median Age', 'Average Household Size']
    demographics = convert_type(demographics, dict(zip(integer_columns, len(integer_columns)*[IntegerType()])))
    demographics = convert_type(demographics, dict(zip(float_columns, len(float_columns)*[DoubleType()])))
    
    # First aggregation on City
    first_aggregation = {"Median Age": "first", "Male Population": "first", "Female Population": "first", 
                 "Total Population": "first", "Number of Veterans": "first", "Foreign-born": "first", "Average Household Size": "first"}
    first_aggregation_df = demographics.groupby(["City", "State", "State Code"]).agg(first_aggregation)
    
    # Pivot Table to convert values of the Race column into different columns
    pivot_table = demographics.groupBy(["City", "State", "State Code"]).pivot("Race").sum("Count")
    
    # Remove the spaces from columns
    demographics = first_aggregation_df.join(other=pivot_table, on=["City", "State", "State Code"], how="inner")\
    .withColumnRenamed('first(Total Population)', 'TotalPopulation')\
    .withColumnRenamed('first(Female Population)', 'FemalePopulation')\
    .withColumnRenamed('first(Male Population)', 'MalePopulation')\
    .withColumnRenamed('first(Median Age)', 'MedianAge')\
    .withColumnRenamed('first(Number of Veterans)', 'NumberVeterans')\
    .withColumnRenamed('first(Foreign-born)', 'ForeignBorn')\
    .withColumnRenamed('first(Average Household Size)', 'AverageHouseholdSize')\
    .withColumnRenamed('Hispanic or Latino', 'HispanicOrLatino')\
    .withColumnRenamed('Black or African-American', 'BlackOrAfricanAmerican')\
    .withColumnRenamed('American Indian and Alaska Native', 'AmericanIndianAndAlaskaNative')
    
    
    # Fill the Na/NaN values with 0
    numeric_columns = ['TotalPopulation', 'FemalePopulation', 'MedianAge', 'NumberVeterans', 'ForeignBorn', 'MalePopulation', 'AverageHouseholdSize',
                    'AmericanIndianAndAlaskaNative', 'Asian', 'BlackOrAfricanAmerican', 'HispanicOrLatino', 'White']
    demographics = demographics.fillna(0, numeric_columns)
    
    # Save the demographics DataFrame to the output_path
    if output_path is not None:
        save_data(df=demographics, output_path=output_path, partitionBy = partitionBy)
    
    return demographics

def process_states_data(spark, input_path, output_path, columns, partitionBy, states_codes,
                                input_format = "csv",  debug_size = None,
                                header=True, sep=";", **options):
    """
    Loads the the states dataset stored in the given input_path, 
    performs the ETL processes and saves the resulted data in the  given output path.
    
    Patameters
    --------------
    spark : obj
        The Spark session, representing the entry point to programming Spark.
    input_path : str
        Directory where the input files are located.
    output_path : str
        The output path were the states output files will be stored. 
    columns : list 
        List of columns names to read in. (Note: Only some columns are useful.)
    partitionBy : list (
        List of partitioning columns names. 
    states_codes : DataFrame
        The demographics dataFrame aggregated by State
    input_format : str
        The format of the input data files. Default value: 'csv'.
    debug_size : int
        The number of rows to returned for debug purposes. 
        Default value: None (e.g. all rows).  
    header : bool
        Header will be used as names of columns. Default value: False.
    options : str
        All the other available string options.
    
    Returns
    --------------
    states : DataFrame
        The states DataFrame
    """
    
    # Loads the lookup table I94ADDR
    states = load_data(spark, input_path="lookup/I94ADDR.csv", input_format="csv", columns="*", header=True)\
    .withColumnRenamed('State', 'State Original')
    
    # Join the states and states_codes datasets
    states = states.join(states_codes, states_codes["State Code"] == states.Code, "left")
    states = states.withColumn("State", when(isnull(states["State"]), capitalize_udf(states['State Original'])).otherwise(states["State"]))
    states = states.drop('State Original', 'State Code')
    
    
    
    # Rename the default columns names returned after Spark aggregation.

    ethn_columns = ['sum(BlackOrAfricanAmerican)', 'sum(White)', 'sum(AmericanIndianAndAlaskaNative)',
            'sum(HispanicOrLatino)', 'sum(Asian)', 'sum(NumberVeterans)', 'sum(ForeignBorn)', 'sum(FemalePopulation)', 
            'sum(MalePopulation)', 'sum(TotalPopulation)']
                        
    mapping_ethn = dict(zip(ethn_columns, [re.search(r'\((.*?)\)', col).group(1) for col in ethn_columns]))
    states = rename_columns(states, mapping_ethn)
    
    # Save the states DataFrame to the output_path
    if output_path is not None:
        save_data(df=states, output_path=output_path)
    
    return states
    
def process_countries_data(spark, input_path, output_path, 
                         input_format = "csv", columns = '*', debug_size = None, header=True, **options):
    """
    Loads the the countries dataset stored in the given input_path, 
    performs the ETL processes and saves the resulted data in the  given output path.
    
    Patameters
    --------------
    spark : obj
        The Spark session, representing the entry point to programming Spark.
    input_path : str
        Directory where the input files are located.
    output_path : str
        The output path were the countries output files will be stored. 
    input_format : str
        The format of the input data files. Default value: 'csv'.
    columns : list 
        List of columns names to read in. (Note: Only some columns are useful.)
    debug_size : int
        The number of rows to returned for debug purposes. 
        Default value: None (e.g. all rows)
    header : bool
        Header will be used as names of columns. Default value: False.
    options : str
        All the other available string options.
    
    Returns
    --------------
    countries : DataFrame
        The countries DataFrame
    """

    # Loads the countries Spark dataframe 
    countries_initial = load_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, debug_size = debug_size, header=header, **options)
    # Aggregates the countries DataFrame by Country and renames the new columns
    countries_initial = countries_initial.groupby(["Country"]).agg({"AverageTemperature": "avg", "Latitude": "first", "Longitude": "first"})\
    .withColumnRenamed('avg(AverageTemperature)', 'Temperature')\
    .withColumnRenamed('first(Latitude)', 'Latitude')\
    .withColumnRenamed('first(Longitude)', 'Longitude')
    
    # Rename specific countries to match the I94CIT_I94RES lookup table in JOIN function 
    renaming_countries = [("Country", "Congo (Democratic Republic Of The)", "Congo"), ("Country", "Côte D'Ivoire", "Ivory Coast")]
    countries_initial = conditional_value_refactoring(countries_initial, renaming_countries)
    countries_initial = countries_initial.withColumn('Country_Lower', lower(countries_initial.Country))
    
    # Rename specific countries to match the demographics DataFrame in JOIN function 
    renaming_countries_dem = [("I94CTRY", "BOSNIA-HERZEGOVINA", "BOSNIA AND HERZEGOVINA"), 
                  ("I94CTRY", "INVALID: CANADA", "CANADA"),
                  ("I94CTRY", "CHINA, PRC", "CHINA"),
                  ("I94CTRY", "GUINEA-BISSAU", "GUINEA BISSAU"),
                  ("I94CTRY", "INVALID: PUERTO RICO", "PUERTO RICO"),
                  ("I94CTRY", "INVALID: UNITED STATES", "UNITED STATES")]
    
    # Loads the lookup table I94CIT_I94RES
    countries = load_data(spark, input_path="lookup/I94CIT_I94RES.csv", input_format=input_format, columns="*",
                          debug_size = debug_size, header=header, **options)
    countries = convert_type(countries, {"Code": IntegerType()})
    countries = conditional_value_refactoring(countries, renaming_countries_dem)
    countries = countries.withColumn('Country_Lower', lower(countries.I94CTRY))
    
    # Join the countries_initial and countries datasets to create the resu;ting country dimmension table
    countries = countries.join(countries_initial, countries.Country_Lower == countries_initial.Country_Lower, how="left")
    countries = countries.withColumn("Country", when(isnull(countries["Country"]), capitalize_udf(countries.I94CTRY)).otherwise(countries["Country"]))   
    countries = countries.drop("I94CTRY", "Country_Lower")
    
    # Save the countries DataFrame to the output_path
    if output_path is not None:
        save_data(df=countries, output_path=output_path)
    
    return countries

def convert_type(df, columns_config):                      
    """
    Converts the types of the columns in the given DataFrame according to the specified configurations 
    in the columns_config dictionary. The format of the columns_config is {"column_name": type}.
    
    Patameters
    --------------
    df : SparkDataFrame
        The Spark dataframe whose columns will be converted. 
    columns_config : dict
        The dictionary containing the configurations. The format is {"column_name": type}
    
    Returns 
    --------------
    df : SparkDataFrame
        The resulting SparkDataFrame with the converted columns. 
    """
    
    for k,v in columns_config.items():
        if k in df.columns:
            df = df.withColumn(k, df[k].cast(v))
    
    return df

def convert_SAS_date(df, SAS_columns):
    """
    Converts the dates in SAS datatype to a string format YYYY-MM-DD.
    
    Patameters
    --------------
    df : SparkDataFrame
        The Spark dataframe whose columns will be converted. 
    SAS_columns : list
        List of columns in SAS date format need to be converted.
    
    Returns 
    --------------
    df : SparkDataFrame
        The resulting SparkDataFrame with the converted columns.
    """
    
    for c in [c for c in SAS_columns if c in df.columns]:
        df = df.withColumn(c, convert_sas_udf(df[c]))
    
    return df

def conditional_value_refactoring(df, conditions):
    """
    Refactores column values based on specfied conditions.
    
    Patameters
    --------------
    df : SparkDataFrame
        The Spark dataframe whose columns will be converted. 
    conditions : list
        List of conditions in tuples format (example: changing_field, old_value, new_value)
     
    Returns 
    --------------
    df : SparkDataFrame
        The resulting SparkDataFrame with the converted columns.
    """
    for changing_field, old_value, new_value in conditions:
        df = df.withColumn(changing_field, when(df[changing_field] == old_value, new_value).otherwise(df[changing_field]))

    return df

def rename_columns(df, renaming_conditions):
    """
    Rename the columns of the DataFrame based in the conditions provided in the renaming_conditions
    dictionary.
    
    Patameters
    --------------
    df : SparkDataFrame
        The Spark dataframe whose columns will be converted. 
    renaming_conditions : dict 
        The dictionary with the given rnaming conditions. The format of the dictionary
        is {old_name: new_name}.
    
    Returns 
    --------------
    df : SparkDataFrame
        The resulting SparkDataFrame with the converted columns.
    """
                        
    df = df.select([col(column).alias(renaming_conditions.get(column, column)) for column in df.columns])
    
    return df

def date_difference(date_value_1, date_value_2):
    """
    Calculates the delta between the values of two dates.
    
    Patameters
    --------------
    date_value_1 : str
        The value of the first date.
    date_value_2 : str
        The value of the second date. 
    
    Returns 
    --------------
    delta : int
        The delta between the values of two dates.
    """
    
    if date_value_2 is None:
        return None
    else:
        a = datetime.strptime(date_value_1, date_format)
        b = datetime.strptime(date_value_2, date_format)
        delta = b - a
        return delta.days

# The date string format  we use in this project is YYYY-MM-DD
date_format = "%Y-%m-%d"

# Spark udf wrapper functions
# Convert dates in SAS format into string format of YYYY-MM-DD. 
convert_sas_udf = udf(lambda x: x if x is None else (timedelta(days=x) + datetime(1960, 1, 1)).strftime(date_format))
# Capitalize the first letter of a given string.
capitalize_udf = udf(lambda x: x if x is None else x.title())
# calculate the delta between two given dates.
date_difference_udf = udf(date_difference)

if __name__ == "__main__" :
    spark = create_spark_session()
    # Perform ETL processes for the given datasets to generate immigration, date, temperature,
    # airport, demographics, states and countries tables. Save the tables in the given S3 bucket (output_path parameters).
    
    ### Immigration and date tables 
    immigration = process_immigration_data(spark, input_path='../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat',
                                           output_path="s3a://udacity-data-engineer-capstone/immigration.parquet",
                                           date_output_path="s3a://udacity-data-engineer-capstone/date.parquet",
                                           input_format = "sas", 
                                           columns = ['i94addr', 'i94mon','cicid','i94visa','i94res','arrdate','i94yr','depdate',
                                                      'airline', 'fltno', 'i94mode', 'i94port', 'visatype', 'gender', 
                                                      'i94cit', 'i94bir'],
                                           debug_size=1000, partitionBy = ["i94yr", "i94mon"], saved_columns = '*', 
                                           header=True, **options)

    ### Temperature table
    temperature = process_temperature_data(spark, input_path="../../data2/GlobalLandTemperaturesByCity.csv", 
                                           output_path="s3a://udacity-data-engineer-capstone/temperature.parquet", 
                                           input_format = "csv", columns = '*', debug_size = None, partitionBy = ["Country", "City"],
                                           header=True, **options)
                        
    ### Airport table
    airport = process_airport_data(spark, input_path="airport-codes_csv.csv", 
                                   output_path="s3a://udacity-data-engineer-capstone/airport.parquet",
                                   input_format = "csv", columns = '*', debug_size = None, 
                                   partitionBy = ["iso_country"], header=True, **options)
   
    ### Demographics table
    demographics = process_demographics_data(spark, input_path="us-cities-demographics.csv",
                                             output_path="s3a://udacity-data-engineer-capstone/demographics.parquet",
                                             input_format = "csv", columns='*',
                                             debug_size = None, partitionBy = ["State Code"], 
                                             header=True, sep=";", **options)
    ### States table
    states_codes = demographics.groupby(["State Code", "State"]).agg(dict(zip(cols, len(cols)*["sum"])))
    states = process_states_data(spark, input_path="lookup/I94ADDR.csv",
                                         output_path="s3a://udacity-data-engineer-capstone/states.parquet",
                                         input_format = "csv", columns= ['TotalPopulation', 'FemalePopulation', 'MalePopulation',
                                                                         'NumberVeterans', 'ForeignBorn',
                                                                         'AmericanIndianAndAlaskaNative', 'Asian', 'BlackOrAfricanAmerican',
                                                                         'HispanicOrLatino', 'White'], 
                                         debug_size = None, partitionBy = ["State Code"], states_codes = states_codes, 
                                         header=True, sep=";", **options)
    ### Countries table
    countries = process_countries_data(spark, input_path="../../data2/GlobalLandTemperaturesByCity.csv", 
                                       output_path="s3a://udacity-data-engineer-capstone/country.parquet",
                                       input_format = "csv", columns = '*', debug_size = None,
                                       header=True, **options)
                    