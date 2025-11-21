package org.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.*;

public class BikeSharingAnalysis {
    public static void main(String[] args) {
        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("BikeSharingAnalysis")
                .master("local[*]")
                .getOrCreate();

        // 1. Data Loading & Exploration
        // Load the CSV file into a Spark DataFrame
        Dataset<Row> bikeDF = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("bike_sharing.csv");

        // Display the schema
        System.out.println("=== Schema ===");
        bikeDF.printSchema();

        // Show the first 5 rows
        System.out.println("=== First 5 Rows ===");
        bikeDF.show(5);

        // How many rentals are in the dataset?
        long rentalCount = bikeDF.count();
        System.out.println("=== Total Rentals ===");
        System.out.println("Total rentals: " + rentalCount);

        // 2. Create a Temporary View
        bikeDF.createOrReplaceTempView("bike_rentals_view");

        // 3. Basic SQL Queries
        System.out.println("=== Rentals longer than 30 minutes ===");
        spark.sql("SELECT * FROM bike_rentals_view WHERE duration_minutes > 30").show();

        System.out.println("=== Rentals starting at Station A ===");
        spark.sql("SELECT * FROM bike_rentals_view WHERE start_station = 'Station A'").show();

        System.out.println("=== Total Revenue ===");
        spark.sql("SELECT SUM(price) as total_revenue FROM bike_rentals_view").show();

        // 4. Aggregation Queries
        System.out.println("=== Rentals per Start Station ===");
        spark.sql("SELECT start_station, COUNT(*) as rental_count " +
                "FROM bike_rentals_view GROUP BY start_station ORDER BY rental_count DESC").show();

        System.out.println("=== Average Rental Duration per Start Station ===");
        spark.sql("SELECT start_station, AVG(duration_minutes) as avg_duration " +
                "FROM bike_rentals_view GROUP BY start_station ORDER BY avg_duration DESC").show();

        System.out.println("=== Station with Highest Number of Rentals ===");
        spark.sql("SELECT start_station, COUNT(*) as rental_count " +
                "FROM bike_rentals_view GROUP BY start_station ORDER BY rental_count DESC LIMIT 1").show();

        // 5. Time-Based Analysis
        // Extract hour from start_time and add as new column
        Dataset<Row> bikeWithHour = bikeDF.withColumn("hour", hour(col("start_time")));
        bikeWithHour.createOrReplaceTempView("bike_rentals_with_hour");

        System.out.println("=== Bike Rentals Per Hour (Peak Hours) ===");
        spark.sql("SELECT hour, COUNT(*) as rental_count " +
                "FROM bike_rentals_with_hour GROUP BY hour ORDER BY rental_count DESC").show();

        System.out.println("=== Most Popular Start Station during Morning (7-12) ===");
        spark.sql("SELECT start_station, COUNT(*) as rental_count " +
                "FROM bike_rentals_with_hour WHERE hour BETWEEN 7 AND 12 " +
                "GROUP BY start_station ORDER BY rental_count DESC LIMIT 1").show();

        // 6. User Behavior Analysis
        System.out.println("=== Average Age of Users ===");
        spark.sql("SELECT AVG(age) as average_age FROM bike_rentals_view").show();

        System.out.println("=== User Count by Gender ===");
        spark.sql("SELECT gender, COUNT(*) as user_count " +
                "FROM bike_rentals_view GROUP BY gender").show();

        System.out.println("=== Rentals by Age Group ===");
        spark.sql("SELECT " +
                "CASE " +
                "  WHEN age BETWEEN 18 AND 30 THEN '18-30' " +
                "  WHEN age BETWEEN 31 AND 40 THEN '31-40' " +
                "  WHEN age BETWEEN 41 AND 50 THEN '41-50' " +
                "  ELSE '51+' " +
                "END as age_group, " +
                "COUNT(*) as rental_count " +
                "FROM bike_rentals_view " +
                "GROUP BY age_group " +
                "ORDER BY rental_count DESC").show();

        // Stop SparkSession
        spark.stop();
    }
}