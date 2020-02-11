# Perpetual Taxi

**Introduction**

This is a project working during the Insight Data Engineer fellowship. The goal is providing taxi driver an estimation of waiting time for his next rider at different region. Taxi driver could use this info to help him better determine his next destination and optimize his earning.

**Data input**

NYC TLC Trip Record Data (https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page). This data contains info includes pick-up and drop-off datetime and location. Where location is recoreded in zones. 

**Approach and method**

Since there is no taxiID in the data, waiting time is not avalible directly from data. The way to estimate waiting time for this project is based on a simulation model. (sudo)Taxi driver distribution will be initialized in the morning at each day. The kafka ingested data is treated as real-time streaming trip request. Each trip request will match with a taxi in data. This taxi will then move to his destination and waiting time is calculate based on the time period between his last drop-off time and next pick-up time.

**Archtechture**

<img src="https://github.com/JelovXCMS/TaxiForNextRide/blob/master/image/archtechture.png" alt="alt text" width="700">

Data is stored in Amaozn S3, ingested by Kafka, then a program consume message from kafka to do the simulation with its taxi driver distribtuion store in redis for fast and scalable operations. The waiting time info is then saved into postgresql and user could access those info via dash.
