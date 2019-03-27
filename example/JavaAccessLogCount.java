import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public final class JavaAccessLogCount {

  public static void main(String[] args) throws Exception {

    SparkSession spark = SparkSession
          .builder()
          .getOrCreate();
    JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

    JavaRDD<String> lines = jsc.textFile("/home/student/dataset/accesslog.csv",4);

    JavaPairRDD<String, Integer> ips = lines.mapToPair( str -> {
       String[] cols = str.split(",");
       return new Tuple2<String, Integer>(cols[0],1);
    });

    JavaPairRDD<String, Integer> counts = ips.reduceByKey( (i,j) -> i+j );

    List<Tuple2<String, Integer>> output = counts.collect();
    for (Tuple2<?,?> tuple : output) {
      System.out.println(tuple._1() + ": " + tuple._2());
    }

    spark.stop();
  }
}
