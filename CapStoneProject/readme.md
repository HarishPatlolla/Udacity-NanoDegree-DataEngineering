# Data Engineering Capstone Project

### Executive Summary

The Business Objective of this project is to support the `analytical datawarehouse in Amazon RedShift( cloud data warehouse)` for `U.S. Customs and Border Protection`. It is meant to process the raw data from the source, transform and load to the datalakes to provide analytics ready data about visitors entering U.S. The analytics team at `U.S. Customs and Border Protection` can build the reports; which can help  for better resource allocation;thereby enhancing the visitors experience hassle-free on their entry into United States.  
### The Architecture

The complete solution is cloud based on top of `Amazon Web Services (AWS)`. 

In this project, the datasets are loaded into EMR Cluster, preprocessed with Apache Spark and stored back as Fact and Dimension Tables in AWS S3 bucket in parquet format. The DataQuality checks are performed on the processed data. The Analytics team can pull data into `OLAP datawarehouse` or `BI apps` for their convenient analysis.

The main objective of this project is to develop a ETL pipeline to process huge amounts of source data using `Apache Spark`, store the staging data back to `Amazon S3` bucket (To allow multiple users across the continum to access concurrently), 

Technologies Used: `Python` `Apache Spark` `Amazon S3` 

![screenshot](architecture.png)

Image Source : https://github.com/fpcarneiro/data-engineer-project/blob/master/Capstone%20Project%20Template.ipynb


### Data Source

`I94 Immigration Data`: 
This data comes from the US National Tourism and Trade Office Source. This data records immigration records partitioned by month of every year.

`World temperature Data`
This dataset comes from Kaggle Source. Includes temperature recordings of cities around the world for a period of time


`US City Demographic Data`
This dataset comes from OpenSoft Source. Includes population formation of US states, like race and gender

####  Conceptual Data Model

STAR SCHEMA has been chosen to maintain the data integrity

After acessing the data from different datasets, we have `Immigration`,`country`,`Demographics` tables 

1. `Immigration Table` - It forms the center of the data warehouse. It is the Fact table consisting information about the 
    visitors information like Arrival Date, Departure Date, Gender, AirLine, Type of Visa
    *Primary Key = cicid ( Unique identifer for the Visitor )*
    
    
2.  ` Date Dimension Table` - From the Immigration table, a date dimension table was created using the Unique 
      arrival and departure dates
      *Primary Key = date*
      
        
3.  `The STATE dimension table` contains aggregation of the demographics dataset by the State column. It contains the 
      overall statistics ( Median Age, Male Population, Female Population, Total Population, Number of Veterans, Foreign-born)
      and the same statistics for each of the race ((BlackOrAfricanAmerican, White, ForeignBorn, AmericanIndianAndAlaskaNative, 
      HispanicOrLatino, Asian)
      *Primary Key State*
      
      
4. `The COUNTRY dimention` completes our star schema model. It has the average temperature , lattitude , longitude 
    for each country
    *Primary key COuntry*

#### ETL Data Pipelines

The `ETL` pipeline to `Extract` the data from the repository, `Transform` using Spark and `Load` into S3 buckets in parquet format
