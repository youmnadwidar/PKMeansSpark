/**
 * Illustrates a KMeans clustering in Java
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.spark.util.Vector;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;


public class PKMeans {

    private String inputFile;
    private int numOfCentroids;
    JavaSparkContext sc;

    PKMeans(String input, String output, int num) {
        this.inputFile = input;
        this.numOfCentroids = num;
        SparkConf conf = new SparkConf().setMaster("local").setAppName("KMeans");
        sc = new JavaSparkContext(conf);
    }

    public JavaPairRDD<Integer, Vector> runKMeans(JavaPairRDD<Integer, Vector> centroids) throws IOException {

        // Load our input data.
        JavaRDD<String> input = sc.textFile(inputFile);

        // Split up line to form a datapoint.
        JavaRDD<Vector> points = input.map(line -> {
            String[] strings = line.split(",");
            double[] point = IntStream.range(0, strings.length - 1).mapToDouble(i -> Double.parseDouble(strings[i])).toArray();
            double[] modifiedPoint = new double[point.length + 1];
            System.arraycopy(point, 0, modifiedPoint, 0, point.length);
            modifiedPoint[modifiedPoint.length - 1] = 1;
            return new Vector(modifiedPoint);
        });
        if (centroids == null) {
            java.util.List<Vector> centroidsRead = points.takeSample(false, numOfCentroids);
            List<Tuple2<Integer, Vector>> centroidsPairs = new ArrayList<Tuple2<Integer, Vector>>();
            int i = 0;
            for (Vector centroid : centroidsRead) {
                centroidsPairs.add(new Tuple2<Integer, Vector>(i, centroid));
                i++;
            }
            centroids = sc.<Integer, Vector>parallelizePairs(centroidsPairs);
        }


        List<Vector> list = centroids.sortByKey().values().collect();

        JavaPairRDD<Integer, Vector> pointsPair = points.mapToPair(point -> {
            int index = Helper.getClosestCentroid(point.elements(), list);
            return new Tuple2<>(index, point);
        });

        JavaPairRDD<Integer, Vector> reducerCentroids = pointsPair.reduceByKey((point, sum) -> {
            double[] total = new double[point.length()];
            total[total.length - 1] = point.apply(point.length() - 1) + sum.apply(sum.length() - 1);
            for (int j = 0; j < point.length() - 1; j++) {
                total[j] = (point.apply(j) * point.apply(point.length() - 1)) + (sum.apply(j) * sum.apply(sum.length() - 1));
                total[j] /= total[total.length - 1];
            }
            return new Vector(total);
        });

        return reducerCentroids;


    }

}