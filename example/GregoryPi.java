import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class GregoryPi {
   public static void main(String[] args) {
      SparkSession spark = SparkSession
            .builder()
            .getOrCreate();
      JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
      
      Integer[] arr = new Integer[100000];
      for (int i=0; i < 100000; i++) arr[i] = i; 

      JavaRDD<Integer> dataRdd = jsc.parallelize(Arrays.asList(arr),4);

      JavaRDD<Double> newRdd = dataRdd.map( i ->  {
         int divisor = 2*i+1;
         if (i % 2 != 0) divisor = -divisor;
         return 1.0/divisor;
      });

      Double sum = newRdd.reduce( (i,j) -> i+j );

      System.out.println(sum*4);

      spark.stop();
   }

}
