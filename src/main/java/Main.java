import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Main {
    private static String HADOOP_COMMON_PATH = "C:\\Dev\\scrm\\src\\main\\resources\\winutils";
    private static String INPUT_FILE = "src/main/resources/events.csv";

    private static String COUNT_COLUMN = "count";
    private static String ACTION_COLUMN = "action";
    private static String TIME_COLUMN = "time";
    public static void main(String[] args) throws Exception {

        System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("SCRM")
                .getOrCreate();

        Dataset<Row> events = spark
                .read()
                .option("header" , "true")
                .csv(INPUT_FILE);

        events.printSchema();
        events.show(20);

        // Aggregate by windows of 10 minutes and get actions per minute of each type
        Dataset<Row> timedWindow = events
                .groupBy(window(events.col(TIME_COLUMN), "10 minutes"), col(ACTION_COLUMN))
                .agg(count(ACTION_COLUMN).as(COUNT_COLUMN))
                .withColumn("perMinute", col(COUNT_COLUMN).divide(10));

        // Get average of actions at each 10-minute window
        Dataset<Row> avg = timedWindow
                .groupBy(col("window"))
                .avg(COUNT_COLUMN).as("averageOfActions");

        // Get top-10 10-minute window with most Open actions
        Dataset<Row> mostOpen = timedWindow
                .filter(col(ACTION_COLUMN).equalTo("Open"))
                .orderBy(desc(COUNT_COLUMN));

        timedWindow.show(50);

        avg.show(50);

        mostOpen.show(10);
    }
}
