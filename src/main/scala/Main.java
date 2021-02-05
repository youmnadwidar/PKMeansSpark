import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.util.Vector;
import scala.Tuple1;
import scala.Tuple2;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.commons.math.util.MathUtils.EPSILON;

public class Main {

    public static void main(String[] args) throws IOException {
/*
        if (args.length != 4) {
            System.out.println("Please enter 4 args as follow : <inputFilePath> <outputFilePath> <K for centroids>");
            System.exit(1);
        }
*/
        args = new String[]{"/home/youmnadwidar/iris.data", "output", "3"};
        String input = args[0];
        String output = args[1];
        int k = Integer.parseInt(args[2]);
        PKMeans kMeans = new PKMeans(input, output, k);
        int max_iterations = 30;
        List<Vector> old_centroids = null;
        List<Vector> newCentroids = null;
        for (int num_iteration = 0; num_iteration < max_iterations; num_iteration++) {
            newCentroids = kMeans.runKMeans(old_centroids, num_iteration);
            if (convergance(old_centroids, newCentroids)) {
                break;
            }
            old_centroids = newCentroids;
        }
        JavaPairRDD<Integer, Iterable<Vector>> lastClusters = kMeans.getLastClustering();
        lastClusters.saveAsTextFile("Last Clusters");
        Iterator<Tuple2<Integer, Iterable<Vector>>> iterator = lastClusters.toLocalIterator();
        List<Double> list = new ArrayList<>();
        while (iterator.hasNext()) {
            Tuple2<Integer, Iterable<Vector>> element = iterator.next();
            double sum = 0;
            int count = 0;
            for (Iterator<Vector> it = element._2.iterator(); it.hasNext(); ) {
                Vector point = it.next();
                double distance = Helper.getEuclideanDistance(point.elements(), newCentroids.get(list.size()).elements());
                sum += Math.sqrt(distance);
                count++;
            }
            list.add(sum/count);
        }
        kMeans.sc.close();
        System.out.println("Euclidean Distances sum");
        for (Double num:list) {
            System.out.println(num+"    ");
        }

    }

    private static boolean convergance(List<Vector> oldCenters, List<Vector> newCenters) {
        if (oldCenters == null || newCenters == null) return false;
        for (int i = 0; i < oldCenters.size(); i++) {
            double[] c1 = oldCenters.get(i).elements();
            double[] c2 = newCenters.get(i).elements();
            for (int j = 0; j < c1.length; j++) {
                if (Math.abs(c1[j] - c2[j]) > EPSILON)
                    return false;

            }

        }
        return true;
    }

}
