# Bike Sharing System Analysis - Spark SQL Project

## Project Overview
This project analyzes public bike-sharing system usage data using Apache Spark SQL to extract insights about user behavior, station popularity, peak hours, and overall system performance.

## Technologies Used
- **Java 8+**
- **Apache Spark 3.4.0**
- **Spark SQL**
- **Maven** (for dependency management)

## Project Structure
```
bike-sharing-analysis/
├── src/
│   └── main/
│       └── java/
│           └── BikeSharingAnalysis.java
├── pom.xml
├── bike_sharing.csv
└── README.md
```

## Dataset Information
The dataset `bike_sharing.csv` contains 100 bike rental transactions with the following columns:
- `rental_id`: Unique ID for each rental
- `user_id`: Unique ID of the user
- `age`: User's age
- `gender`: M or F
- `start_time`: Rental start timestamp
- `end_time`: Rental end timestamp
- `start_station`: Where the bike was picked up
- `end_station`: Where the bike was returned
- `duration_minutes`: Length of the trip
- `price`: Rental cost in dollars

## Prerequisites
- Java 11
- dependecies :
  ```
  <dependencies>

        <!-- Apache Spark Core -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-core_2.12</artifactId>
            <version>3.5.1</version>
        </dependency>

        <!-- Apache Spark SQL -->
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.12</artifactId>
            <version>3.5.1</version>
        </dependency>

        <!-- Logging -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>2.0.9</version>
        </dependency>
    </dependencies>
  ```

  




## Analysis Features

### 1. Data Loading & Exploration
- Load CSV data into Spark DataFrame
- Display schema and sample data
<img width="564" height="354" alt="image" src="https://github.com/user-attachments/assets/cb84ca02-e606-4ccb-8266-13abae3861a2" />
<img width="1213" height="270" alt="image" src="https://github.com/user-attachments/assets/9c3eccd6-25d3-4f1f-bd39-562e363d8747" />

- Count total rentals
<img width="454" height="82" alt="image" src="https://github.com/user-attachments/assets/f5f0c326-a774-465a-8605-7df8048f8aa4" />


### 2. Basic SQL Queries
- Rentals longer than 30 minutes
<img width="1213" height="654" alt="image" src="https://github.com/user-attachments/assets/8c123296-e0d0-4d04-ab86-cb809b3dc829" />

- Rentals starting at stations A
<img width="1203" height="647" alt="image" src="https://github.com/user-attachments/assets/07e19a1d-ef5c-4462-8be7-8f29df8ae222" />

- Total revenue calculation
<img width="182" height="136" alt="image" src="https://github.com/user-attachments/assets/9aad6e18-6fc8-49c8-8e24-d9445551a5ce" />


### 3. Aggregation Analysis
- Rental counts per station
<img width="317" height="252" alt="image" src="https://github.com/user-attachments/assets/e87b26c3-ac66-46fc-814c-af658945343e" />

- Average duration per station
<img width="354" height="253" alt="image" src="https://github.com/user-attachments/assets/5f497c4c-9730-4abc-8ec2-f859f2946511" />

- Most popular stations
<img width="309" height="131" alt="image" src="https://github.com/user-attachments/assets/eba6bf56-00e7-4d47-b09c-ba0375332700" />


### 4. Time-Based Analysis
- Peak hour identification
<img width="217" height="472" alt="image" src="https://github.com/user-attachments/assets/40184c08-0ffb-4c00-8179-dfe526f3f375" />

- Morning usage patterns (7 AM - 12 PM)
<img width="304" height="142" alt="image" src="https://github.com/user-attachments/assets/c5dcd54c-6ee7-4003-a610-c402aeb84736" />


### 5. User Behavior Analysis
- Average user age
<img width="150" height="137" alt="image" src="https://github.com/user-attachments/assets/33f21de2-cf84-4b9a-bdf3-dfa65319ba87" />

- Gender distribution
  <img width="208" height="163" alt="image" src="https://github.com/user-attachments/assets/bf9ace87-2a7b-4ffd-837e-bda57515f53f" />



- Age group analysis (18-30, 31-40, 41-50, 51+)
<img width="253" height="190" alt="image" src="https://github.com/user-attachments/assets/85e70cd8-313d-43f0-bdca-f0f28a8f2568" />





