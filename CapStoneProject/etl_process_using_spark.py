import os, re
import configparser
from datetime import timedelta, datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, when, lower, isnull, year, month, dayofmonth, hour, weekofyear, dayofweek, date_format,to_date
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType

# The date format used: YYYY-MM-DD
date_format = "%Y-%m-%d"

# The AWS key id and password are configured in a configuration file "dl.cfg"
config = configparser.ConfigParser()
config.read('config.cfg')

# Reads and saves the AWS access key information and saves them in a environment variable
os.environ['AWS_ACCESS_KEY_ID']=config['AWS_KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_KEYS']['AWS_SECRET_ACCESS_KEY']

#OUTPUT = config['ETL']['OUTPUT_DATA']


##########################################################################################################
################################################# User Defined Functions #################################
##########################################################################################################



def rename_columns(df, dict_map):
    '''
    Rename the columns of the dataset
    Arguments:
        df      : Spark dataframe to be processed.
        dict_map: key=old_name value= new_name
    '''
    df = df.select([col(c).alias(dict_map.get(c, c)) for c in df.columns])
    return df


def cast_type(df, dict_obj):
    """
    Converts the column to the desired type
    Arguments:
        df       : Spark dataframe .
        dict_obj : Dictionary object ( Key= column name ; value = Type to be converted to)     """

    #k=Key (Column name) ; #v=Value (Type)
    for k,v in dict_obj.items():
        if k in df.columns:
            df = df.withColumn(k, df[k].cast(v))
    return df



def convert_sas_date(df, cols):
    """
    Convert dates in the SAS datatype to a date in a string format YYYY-MM-DD 
    Args:
        df   : Spark Dataframe
        cols : List of columns in the SAS date format to be convert
    """
    
    #user defined function 
    convert_sas_udf = udf(lambda x: x if x is None 
                                      else (timedelta(days=x) + datetime(1960, 1, 1)) .date().strftime(date_format))
   
    
    for c in [c for c in cols if c in df.columns]:
        df = df.withColumn(c, convert_sas_udf(df[c]))
    return df


def date_diff(date1, date2):
    '''
    Calculates the difference in days between two dates
    '''
    if date2 is None:
        return None
    else:
        a = datetime.strptime(date1, date_format)
        b = datetime.strptime(date2, date_format)
        delta = b - a
        return delta.days


def change_field_value_condition(df, change_list):
    '''
    Helper function used to rename column values based on condition.
    
    Args:
        df (:obj:`SparkDataFrame`): Spark dataframe to be processed.
        change_list (:obj: `list`): List of tuples in the format (field, old value, new value)
    '''
    for field, old, new in change_list:
        df = df.withColumn(field, when(df[field] == old, new).otherwise(df[field]))
    return df




# User defined functions using Spark udf wrapper function to convert SAS dates into string dates in the format YYYY-MM-DD, to capitalize the first letters of the string and to calculate the difference between two dates in days.
capitalize_udf = udf(lambda x: x if x is None else x.title())
date_diff_udf = udf(date_diff)

#######################################################################
########################### Function to create the spark session  ######
#######################################################################

def create_spark_session():
    """
    This function creates a Spark Session"""
    spark = SparkSession.builder.config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11,org.apache.hadoop:hadoop-aws:2.7.0")\
    .enableHiveSupport().getOrCreate()
    return spark



###################################################################################
########################### Function to read the data into spark from source  ######
####################################################################################


def read_data(spark, input_path, input_format = "csv", columns = '*', **options):
    """
    Loads data from a data source and returns a spark DataFrame
    Arguments: 
        spark        : Spark session. 
        input_path   : Directory where the input files resides.
        input_format : File Format (Default to 'csv')
        columns      : List of columns of the dataframe to return. Default to "*" ('all columns')
        records_to_review : Default=None. Number of records to read to check
        options      : It is **kwargs(varying keyword arguments) . It can be input additional string options
    """
    df = spark.read.load(input_path, format=input_format, **options).select(columns)
    return df


#######################################################################################
########################### Function to save the data from spark into s3 bucket  ######
#######################################################################################


def save_data(df, output_path, mode = "overwrite", output_format = "parquet", columns = '*', partitionBy=None, **options):
   
    """ Exports the contents of the spark dataframe to a file of mentioned source.
  
    Arguments:
    
        df              : Spark DataFrame.
        output_path     : The path where the DataFrame should be saved.
        mode            : Default to 'overwrite'.Specifies the behavior of the save operation when data already exists. 
        output_format   :  Default to 'parquet'. format of the data source to be saved.
        columns         : List of columns of the dataframe to save. Default to "*", which means 'all columns'.
        partitionBy     : Names of columns to be partioned by . Parquet files are partitioned by default.
                         None means 'no partitions'.
        options         : All additional options.
    """

    df.select(columns).write.save(output_path, mode= mode, format=output_format, partitionBy = partitionBy, **options)
    return df


# ################################################################################################################
# ########################### Function to process immigration file into Immigration Fact and Date Dimension  ######
# ################################################################################################################

# Using the 2016 April File 
# ../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat

def etl_immigration_data(spark, input_path="immigration_data_sample.csv",
                         output_path="out/immigration.parquet",
                         date_output_path="out/date.parquet",
                         input_format = "csv",
                         columns = '*', 
                         load_size = None, 
                         partitionBy = ["i94yr", "i94mon"],
                         columns_to_save='*', 
                         header=True, **options):
   
    """This Function loads the file and processes into Immigration Fact and Dimension Table """

    
    # Discarded the Multiple columns as there seemed not very useful.
    #['admnum', 'biryear', 'count', 'dtaddto', 'dtadfile', 'entdepa', 'entdepd', 'entdepu', 'insnum', 'matflag', 'occup', 'visapost'] 
    
  
    # Loads the immigration dataframe using Spark
    immigration = read_data(spark, input_path=input_path, input_format=input_format, 
                            columns=columns, header=header, **options)
    
    print("##################### Immigration DataFrame Loaded in Spark \n")
    
    #Numeric columns
    integer_cols = ['cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'arrdate', 'i94mode', 'i94bir', 'i94visa', 'count', 
                    'biryear', 'dtadfile', 'depdate']
    
    #Date Columns
    date_cols = ['arrdate', 'depdate']
    
    #These are the columns with high number of null values
    high_null = ["visapost", "occup", "entdepu", "insnum"]
    
    #These are not very useful columns
    not_useful_cols = ["count", "entdepa", "entdepd", "matflag", "dtaddto", "biryear", "admnum"]
    
    # Type Casting columns to integer
    immigration = cast_type(immigration, dict(zip(integer_cols, len(integer_cols)*[IntegerType()])))
    
    # Convert SAS date to a format of YYYY-MM-DD
    immigration = convert_sas_date(immigration, date_cols)
    
    # Drop high null columns and not useful columns
    immigration = immigration.drop(*high_null)
    immigration = immigration.drop(*not_useful_cols)
   
    # Create a new columns to store the length of the visitor stay in the US
    #Stay= Departure Date - Arrival Date
    immigration = immigration.withColumn('stay', date_diff_udf(immigration.arrdate, immigration.depdate))
    
    #Converting the derived column to integer
    immigration = cast_type(immigration, {'stay': IntegerType()})
    
    #################################################################
    # Creating a Date Dimension Table ###############################
    ################################################################
    
    print('################### \n Creating a Dates Dimension Table \n')
    
    if date_output_path is not None:
        #Taking distinct arrival dates
        arrdate = immigration.select('arrdate').distinct()
        #Taking distinct departure dates
        depdate = immigration.select('depdate').distinct()
        #Union 
        dates = arrdate.union(depdate)
        print(dates.columns)
        dates = dates.withColumn("year", year(dates.arrdate))
        dates = dates.withColumn("month", month(dates.arrdate))
        dates = dates.withColumn("day", dayofmonth(dates.arrdate))
        dates = dates.withColumn("weekofyear", weekofyear(dates.arrdate))
        dates = dates.withColumn("dayofweek", dayofweek(dates.arrdate))
        dates = dates.drop("date").withColumnRenamed('arrdate', 'date')
        
        dates.show(5)
        
        print('################### Loading dates dimension into S3 \n ')
        
        #saving the dataframe to S3 as parquet file
        save_data(df=dates.select("date", "year", "month", "day", "weekofyear", "dayofweek"), output_path=date_output_path)
    
        print('#################### Dates DImension Loaded into s3 \n')
    
    
    print('################ Loading Immigration DataFrame into s3 \n ')
    
    # Save the processed immigration dataset to the output_path
    if output_path is not None:
        
        #saving the dataframe to S3 as parquet file
        save_data(df=immigration, output_path=output_path, partitionBy = partitionBy)
        
        print('#################### Immigration DataFrame Loaded into s3 \n ')
        
    return immigration




# ###############################################################################################################
# ########################### Function to process Countries DataFrame into S3  ######
# ################################################################################################################

def etl_countries_data(spark, 
                       input_path="../../data2/GlobalLandTemperaturesByCity.csv",
                       output_path="out/country.parquet", 
                       input_format = "csv",
                       columns = '*', 
                       load_size = None, 
                       header=True, 
                       **options):
    """
    Reads the global temperatures dataset indicated in the input_path and transform it to generate the country dataframe. Performs the ETL process and saves it in the output path indicated by the parameter out_put path."
    """
    
    print('################### Loading Countries Started \n')
    
    # Loads the demographics dataframe using Spark
    countries = read_data(spark, 
                          input_path=input_path, 
                          input_format=input_format, 
                          columns=columns, 
                          header=header, 
                          **options)
    
    
    countries.show(2)
    
    print('################### \n Loading Countries Done \n')
    
    
    print('################## \n Aggregating Data by Country \n')
    
    # Aggregates the dataset by Country and rename the name of new columns
    countries = countries.groupby(["Country"]).agg( \
                                {"AverageTemperature": "avg", "Latitude": "first", "Longitude": "first"})\
    .withColumnRenamed('avg(AverageTemperature)', 'Temperature')\
    .withColumnRenamed('first(Latitude)', 'Latitude')\
    .withColumnRenamed('first(Longitude)', 'Longitude')
    
    countries.show(2)
    
    print('######################## Renaming the countries \n')
    
    # Rename countries to match the lookup datasets
    change_countries = [("Country", "Congo (Democratic Republic Of The)", "Congo"), 
                        ("Country", "Côte D'Ivoire", "Ivory Coast")]
    countries = change_field_value_condition(countries, change_countries)
    countries = countries.withColumn('Country_Lower', lower(countries.Country))
    
    # Loads the lookup table I94CIT_I94RES
    lookup_table_country_codes = read_data(spark, input_path="LookUpTables/I94CIT_I94RES.csv", input_format=input_format, columns="*",
                           header=header, **options)
    
    lookup_table_country_codes = cast_type(lookup_table_country_codes, {"Code": IntegerType()})
    
    change_res = [("I94CTRY", "BOSNIA-HERZEGOVINA", "BOSNIA AND HERZEGOVINA"), 
                  ("I94CTRY", "INVALID: CANADA", "CANADA"),
                  ("I94CTRY", "CHINA, PRC", "CHINA"),
                  ("I94CTRY", "GUINEA-BISSAU", "GUINEA BISSAU"),
                  ("I94CTRY", "INVALID: PUERTO RICO", "PUERTO RICO"),
                  ("I94CTRY", "INVALID: UNITED STATES", "UNITED STATES")]

    
    lookup_table_country_codes = change_field_value_condition(lookup_table_country_codes, change_res)
    lookup_table_country_codes = lookup_table_country_codes.withColumn('Country_Lower', lower(lookup_table_country_codes.I94CTRY))
    # Join the two datasets to create the country dimmension table
    lookup_table_country_codes = lookup_table_country_codes.join(countries, lookup_table_country_codes.Country_Lower == countries.Country_Lower, how="left")
    lookup_table_country_codes = lookup_table_country_codes.withColumn("Country", when(isnull(lookup_table_country_codes["Country"]), capitalize_udf(lookup_table_country_codes.I94CTRY)).otherwise(lookup_table_country_codes["Country"]))   
    lookup_table_country_codes = lookup_table_country_codes.drop("I94CTRY", "Country_Lower")
    
    print(lookup_table_country_codes.columns)
    
    lookup_table_country_codes.show(2)
    
    print('############# Writing countries file to the S3 STARTED \n')
    
    # Save the resulting dataset to the output_path
    if output_path is not None:
        save_data(df=lookup_table_country_codes, output_path=output_path)
    return res
    
    print('############# Writing countries file to the S3 DONE  \n')
    


################################################################################################################
########################### Function to process Demographics DataFrame into S3  ######
################################################################################################################

def etl_demographics_data(spark, 
                          input_path="us-cities-demographics.csv", 
                          output_path="out/demographics.parquet", 
                          input_format="csv", 
                          columns='*',
                          load_size = None, 
                          partitionBy =["State Code"],
                          header=True, 
                          sep=";", 
                          **options):

    """This Function loads the us-cities-demographics.csv file and stores it to S3 """
        

    #Loads the demographics dataframe using Spark
    demographics = read_data(spark,
                             input_path=input_path,
                             input_format=input_format,
                             columns=columns, 
                             header=header,
                             sep=sep,
                             **options)
    
    # Convert numeric columns to the proper types: Integer and Double
    integer_cols=['Count', 'Male Population', 'Female Population', 'Total Population', 'Number of Veterans', 
                    'Foreign-born']
    float_cols=['Median Age', 'Average Household Size']
    
    demographics = cast_type(demographics, dict(zip(integer_cols, len(integer_cols)*[IntegerType()])))
    demographics = cast_type(demographics, dict(zip(float_cols, len(float_cols)*[DoubleType()])))

    print('#################### \n Aggregating the data \n')
    
    
    #Aggregating the data by CITY and STATE
    aggregate_df = demographics.groupby(["City", "State", "State Code"]).agg({"Median Age": "first", 
                 "Male Population": "first",
                 "Female Population": "first", 
                 "Total Population": "first", 
                 "Number of Veterans": "first",
                 "Foreign-born": "first", 
                 "Average Household Size": "first"})
    
    
    # Pivot Table to transform values of the column Race to different columns
    pivoted_df = demographics.groupby(["City", "State", "State Code"]).pivot("Race").sum("Count")
    
    # Rename column names
    # Join the aggregated Df To Pivoted DF
    demographics = aggregate_df.join(other=pivoted_df, on=["City", "State", "State Code"], how="inner")\
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
    
    
    numeric_cols = ['TotalPopulation', 'FemalePopulation', 'MedianAge', 'NumberVeterans', 'ForeignBorn', 'MalePopulation', 
                    'AverageHouseholdSize','AmericanIndianAndAlaskaNative', 'Asian', 'BlackOrAfricanAmerican', 
                    'HispanicOrLatino', 'White']
    
    # Fill the null values with 0
    demographics = demographics.fillna(0, numeric_cols)
    
    print(demographics.show(2))
    print('\n')
    
    # Save the demographics dataset to the output_path
    if output_path is not None:
        save_data(df=demographics, output_path=output_path, partitionBy = partitionBy)
    
    return demographics



################################################################################################################
########################### Function to process States DataFrame into S3  ######
################################################################################################################


def etl_states_data(spark, output_path="out/state.parquet"):
    
    """This Function uses the demographics data and converts it by state and loads into S3 """
    
    
    cols = ['TotalPopulation', 'FemalePopulation', 'MalePopulation', 'NumberVeterans', 'ForeignBorn', 
            'AmericanIndianAndAlaskaNative', 'Asian', 'BlackOrAfricanAmerican', 'HispanicOrLatino', 'White']

    print('#### Calling etl_demographics_data to load demographics data  as Spark DataFrame \n')
    
    # Loads the demographics dataframe using Spark
    demographics = etl_demographics_data(spark,output_path=None)
    
    print('##### Cursor back to etl_states_data Function \n ')
    
    # Aggregates the dataset by State
    states = demographics.groupby(["State Code", "State"]).agg(dict(zip(cols, len(cols)*["sum"])))
    
    print('###### Extracting State Name from State Code using the Lookup Table \n ')
    
    # Loads the lookup table I94ADDR
    addr = read_data(spark, input_path="LookUpTables/I94ADDR.csv", input_format="csv", columns="*", header=True)\
    .withColumnRenamed('State', 'State Original')
    
    print(addr.columns)
    
    # Join the two datasets
    addr = addr.join(states, states["State Code"] == addr.Code, "left")
    
    addr = addr.withColumn("State", when(isnull(addr["State"]), capitalize_udf(addr['State Original'])).otherwise(addr["State"]))
    
    addr = addr.drop('State Original', 'State Code')
    
    cols = ['sum(BlackOrAfricanAmerican)', 'sum(White)', 'sum(AmericanIndianAndAlaskaNative)',
            'sum(HispanicOrLatino)', 'sum(Asian)', 'sum(NumberVeterans)', 'sum(ForeignBorn)', 'sum(FemalePopulation)', 
            'sum(MalePopulation)', 'sum(TotalPopulation)']
    
    # Rename the columns to modify default names returned when Spark aggregates the values of the columns.
    # For example: column 'sum(MalePopulation)' becomes 'MalePopulation'
    mapping = dict(zip(cols, [re.search(r'\((.*?)\)', c).group(1) for c in cols]))
    addr = rename_columns(addr, mapping)
    
    addr.show(2)
    
    
    print('####################### Loading states dataframe into S3 \n')
    # Save the resulting dataset to the output_path
    if output_path is not None:
        save_data(df=addr, output_path=output_path)
        print('####################### Loaded states dataframe into S3 \n')
    return addr

    
########################################################################################################################
#########################################   Main Function ############################################################    
########################################################################################################################    
    
if __name__ == "__main__" :
    
    
    print('############### Creating Spark Session ')
    
    spark = create_spark_session()
    
    print('\n ############### Spark Session Created ')
    
    
    # Perform ETL process for the Immigration dataset generating immigration and date tables
    #and save them in the S3 bucket indicated in the output_path parameters.
    
    
    print('############### Processing Immigration Data Using Spark \n ')
    
    immigration = etl_immigration_data(spark, 
                                     input_path='../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat',
                                     output_path="s3a://spark-s3-harish/immigration.parquet",
                                     date_output_path="s3a://spark-s3-harish/date.parquet",
                                     input_format = "com.github.saurfang.sas.spark", 
                                     load_size=1000,
                                     partitionBy=None, 
                                     columns_to_save = '*')
    
    print('###############  ETL Processing of Immigration is Done \n ')
    
    
    print('############### Processing Country Data Using Spark  \n ')
    
    # Perform ETL process for the Country table. Generating the Country table and
    #saving it in the S3 bucket indicated in the output_path parameter.
    
    countries = etl_countries_data(spark, output_path="s3a://spark-s3-harish/country.parquet")
    
    print('###############  ETL Processing of Country is Done  \n ')
    
    
    # Perform ETL process for the State table. Generating the State table and saving it 
    #in the S3 bucket indicated in the output_path parameter.
    
    
    print('############### Processing States Data Using Spark \n ')
    
    states = etl_states_data(spark, output_path="s3a://spark-s3-harish/state.parquet")
    
    
    print('###############  ETL Processing of States is Done \n ')
    
