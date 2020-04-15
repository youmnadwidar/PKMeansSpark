/**
 * Illustrates a KMeans clustering in Java
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;


public class PKMeans {

    private String inputFile;
    private String outputFile;
    private int numOfCentroids;
    private JavaSparkContext sc;

    PKMeans(String input, String output, int num) {
        this.inputFile = input;
        this.outputFile = output;
        this.numOfCentroids = num;
        SparkConf conf = new SparkConf().setMaster("local").setAppName("KMeans");
        sc = new JavaSparkContext(conf);
    }

    public void runKMeans() throws IOException {

        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);

        // Split up line to form a datapoint.
        JavaRDD<double[]> points = input.map(line -> {
            String[] strings = line.split(",");
            double[] point = IntStream.range(0, strings.length - 1).mapToDouble(i -> Double.parseDouble(strings[i])).toArray();
            return point;
        });

        java.util.List<double[]> centroidsRead = points.takeSample(false,numOfCentroids);
        List<Tuple2<Integer, double[]>> centroidsPairs = new ArrayList<Tuple2<Integer, double[]>>();
        int i = 0;
        for (double[] centroid : centroidsRead) {
            centroidsPairs.add(new Tuple2<Integer, double[]>(i, centroid));
            i++;
        }
        JavaPairRDD<Integer, double[]> centroids = sc.<Integer, double[]>parallelizePairs(centroidsPairs);
        List<double[]> list = centroids.sortByKey().values().collect();

        JavaPairRDD<Integer, double[]> pointsPair = points.mapToPair(point -> {
            int index = Helper.getClosestCentroid(point, list);
            return new Tuple2<>(index, point);
        });

        JavaPairRDD<Integer, double[]> reducerCentroids = pointsPair.reduceByKey((point, sum) -> {
            double[] total = new double[point.length];
            for (int j = 0; j < point.length; j++) {
                total[j] = point[j] + sum[j];
            }
            return total;
        });
        List list1 = reducerCentroids.sortByKey().values().collect();
        System.out.println(list1);


        this.sc.close();


    }

}