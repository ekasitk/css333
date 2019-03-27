import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class Increment {
   public static void main(String[] args) {
      SparkSession spark = SparkSession
            .builder()
            .getOrCreate();
      JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

      
      JavaRDD<String> dataRdd = jsc.textFile("/home/student/dataset/ones.txt",2);

      JavaRDD<Integer> newRdd = dataRdd.map( str -> 
            Integer.parseInt(str) + 1
      ); 

      Integer sum = newRdd.reduce( (i,j) -> i+j );
      System.out.println("Summation of all numbers = " + sum);
   
      System.out.println("Save newRdd to file");
      newRdd.saveAsTextFile("/home/student/output");

      spark.stop();
   }

}
