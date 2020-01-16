# Does Diversity Attract Diversity: Business Travellers to U.S. Cities
### Data Engineering Capstone Project

#### Project Summary


The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Wrap Up, Future Scenarios, and Further Thoughts

The files in this respository are:
* [etl.py](https://github.com/flyersworder/udacity-capstone-usimmigration/blob/master/etl.py): Execute the ETL process plus some quality checks. Specifically, I use Spark to retreive data from different sources, process the data, and write them into the data lake as parquet files.
* [analysis.py](https://github.com/flyersworder/udacity-capstone-usimmigration/blob/master/analysis.py): An attempt to utilize the data to do some analysis. I create profiles for both business immigrants and cities, connect the tables through the airport codes, and explore the correlations between various features.
* [This jupyter notebook](https://github.com/flyersworder/udacity-capstone-usimmigration/blob/master/Capstone%20Project%20Thought%20Process.ipynb) is a draft that illustrates the whole thought process.

### Step 1: Scope the Project and Gather Data

#### Scope 
The United States is famous for its diversity, a melting pot or a dream that attracts people from different origins and backgrounds. We always think that diversity is beneficial, yet we don't have that many proofs from an economic perspective, particularly from a city level. This project intends to show some evidence on this regard by aggregating the business travellers to the U.S. and associate their diversity to the characteristics of the city. It thus shows us some insights on whether diversity indeed attracts diversity.

#### Describe and Gather Data 
The datasets are provided by Udacity. I draw data from three sources.

* **I94 Immigration Data**: This data comes from the US National Tourism and Trade Office. [This](https://travel.trade.gov/research/reports/i94/historical/2016.html) is where the data comes from. For this project, I only use the immigrants who enter the US for business purpose.
* **U.S. City Demographic Data**: This data comes from [OpenSoft](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). I use it to extract the demographic information of various cities in the U.S..
* **Airport Code Table**: This is a simple table of airport codes and corresponding cities and it comes from [here](https://datahub.io/core/airport-codes#data). It is for bridging the data from previous two sources.

### Step 2: Explore and Assess the Data
#### Explore the Data 
In this session I identify data quality issues, like missing values, duplicate data, etc for each tables, i.e., business_travellers_table, city_demographics_table, and airport_codes_table.

#### Cleaning Steps
1. The business travellers table
    * **Origins**: The origins (*i94cit* column) of these business travellers contain many invalid codes. Since we can't cateogrize these origins, we can either group them into a new category called 'unknow' or filter them out. It is difficult to peer into this unknown category so I decide to get rid of it. Beside, the portion of this category is very small (around 0.3%). 
    * **Age**: For age there are also null values and it is very difficult to interpret. But luckily it is also a very small portion so I can easily remove it.
    * **Gender**: There are also many null values in gender. Since people may have various reasons to not to reveal their genders, and it is largely a self-identified issue, I keep the categories intact.
    * Problem of multiple entries. Unfortunately, we miss a unique identifier for each person. For some reason the long cicid field does not persist in the sas file. But luckily it also makes sense for diversity calculation: if a place constantly attracts like-minded people, it is less diverse.

2. The city demographics table
    * For this table, there are also some null values, for example, in the columns such as *Male Population*, *Female Population*, and *Foreign-born*. Since there are only a few cases, I decide to delete them from the dataset.

3. The airport codes table
    * There are also some null values in this table, for example, in the columns *iata_code* and *municipality*. Since I need to use all the three columns for matching, I can only drop all the null values.

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
The conceptual data model includes two dim tables, i.e., *airport codes table* and *city demographics table*, and one fact table, i.e., *business travellers table*. Because I aim for a longitudinal study, in the long term the *city demographics table* should also become a fact table with annual data. 

#### 3.2 Mapping Out Data Pipelines
I use pyspark to read the data from the source (ideally from s3 buckets), process the data, and output the tables as parquet files to another s3 buckets. Essentially it is a data lake that read different kinds of files (e.g., sas and csv) and then store them into the s3 buckets.

### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
The data model has already been structured in the previous steps inside the data processing enabled by pyspark. The only step missing is to export these processed files as parquets to the s3 buckets.

![ERD](https://github.com/flyersworder/udacity-capstone-usimmigration/blob/master/ERD.png)

#### 4.2 Data Quality Checks
There are several ways to do the quality checks for our datasets:
 * Check there are still null values in the tables after the cleaning process. We could assume that there won't be any null values except for the *gender* field.
 * Check the differences in the number of records between different tables. We could assume that we have much more airports than the ones that receive immigrants, and that we have much more cities than airports.
 * Check whether the travellers are unique: whether there is any duplicates in their ids.
 
#### 4.3 Data dictionary 
In this session I create a data dictionary to explain the fields in all the tables.

#### 1. Business travellers table
- cicid: It is supposed to be some unique identifier to each immigrant, but for some reason it is deprecated in the sas files.
- year: Entering year.
- month: Entering month.
- port: Codes for the entering airport.
- origin: Origins of the immigrant. Each number respresents a different origin.
- age: Age of the immigrant.
- gender: Gender of the immigrant.

#### 2. City demographics table
- city: Name of the city.
- state: Two-letter state code.
- median_age: The median age of the city residents.
- male_population: Male population of the city.
- female_population: Female population of the city.
- total_population: Total population of the city.
- foreign_born: Number of foreign born residents.
- race: Race category.
- count: Population for each race category.

#### 3. Airport codes table
- iata_code: Airport codes.
- region: Two-letter state code.
- municipality: City name.

#### 4.4 Data analysis
I attempt to do some data analysis, primarily calculating different diversity scores for both the business immigrants and the U.S. cities. I then connect these two tables through the airports table and illustrate whether there is indeed a trend.
For the method to calculate the diversity score, I simply adopt the [Herfindahl Index](https://en.wikipedia.org/wiki/Herfindahl%E2%80%93Hirschman_Index).
In this attempt, I will simply compare the origins of business immigrant to the races in the local community.

Later on, I figure that we can do more and gain more insights from this data. So I create a profile for business immigrants, including the features such as the diversity of their origins, total number of business immigrants, and their median age per port. Similarly, I also create a profile for cities, investigating the median age of their residents and the diversity of residents' races. I then calculate the correlation between these two profiles and plot a heatmap. Unsurpringly there are not much useful insights given these cross-sectional data. The most interesting insights may be the negative correlations between the two diversity scores and the total number of immigrants. A high degree of diversity may cause some barriers to hinder the incoming immigrants.

![heatmap](https://github.com/flyersworder/udacity-capstone-usimmigration/blob/master/heatmap.png)

### Step 5: Wrap Up, Future Scenarios, and Further Thoughts
* *The rationale for the choice of tools and technologies for the project*. \
Firstly, I choose to focus on the business travellers because they are less likely to be influenced by factors such as ticket prices and weather and more likely to be attracted by the characteristics of the local community.
Secondly, I use Apache Spark to build a data lake for the variety of different data sources (and potentially more sources in the future) and also its scalability. Using cloud storage such as Amazon S3 makes this solution highly scalable and adaptable, given both the increase of data amount and users.
* *Data and table renew and update*. \
I plan to do a longitudinal study with annual data. So ideally the model should be updated yearly. However, I still can't find a reliable source for the city demographics data. It is a huge effort to collect this data.
Ideally, with enough annual data, we can perform more sophisticated analyses such as [DID](https://en.wikipedia.org/wiki/Difference_in_differences) and certain [matching methods](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC2943670/) to reveal the causal relationship.

**Some future scenarios**:
- *The data was increased by 100x*. \
The current solution with Spark and S3 is quite scalable for big data. We only need to increase the computing power and cloud storage in this case.
- *The data populates a dashboard that must be updated on a daily basis by 7am every day*. \
It is almost a completely different usage. In this case we probably are more interested in the daily imports and exports of immigrants and thus need to focus on other variables in the data such as arrival dates and departure dates. Assuming that this data is updated daily as sas files, we need to schedule a daily data extraction with tools such as Apache Airflow to retreive and process the data and do some quality checks. Luckily, current ETL process is highly modularized and can be easily modified to run as daily tasks in Airflow.
- *The database needed to be accessed by 100+ people*. \
I think it can be well handled by Amazon S3.
