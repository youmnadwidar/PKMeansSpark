import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.util.Vector;

import java.io.IOException;
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
        JavaPairRDD<Integer, Vector> old_centroids = null;
        for (int num_iteration = 0; num_iteration < max_iterations; num_iteration++) {
            JavaPairRDD<Integer, Vector> newCentroids = kMeans.runKMeans(old_centroids);
            newCentroids.saveAsTextFile(output + "_" + num_iteration);
            if (convergance(old_centroids, newCentroids)) {
                return;
            }
            old_centroids = newCentroids;
        }
        kMeans.sc.close();

    }

    private static boolean convergance(JavaPairRDD<Integer, Vector> old_centroids, JavaPairRDD<Integer, Vector> newCentroids) {
        if (old_centroids == null || newCentroids == null) return false;
        List<Vector> oldCenters = old_centroids.sortByKey().values().collect();
        List<Vector> newCenters = old_centroids.sortByKey().values().collect();
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
