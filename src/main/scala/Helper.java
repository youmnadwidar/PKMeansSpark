import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.Vector;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Helper {


    public static int getClosestCentroid(double[] dataPoint, List<Vector> centroids) {
        double minDistance = Integer.MAX_VALUE;
        int minIndex = -1;
        for (int i = 0; i < centroids.size(); i++) {
            double distance = getEuclideanDistance(dataPoint, centroids.get(i).elements());
            if (distance < minDistance) {
                minDistance = distance;
                minIndex = i;
            }
        }
        return minIndex;

    }

    public static double getEuclideanDistance(double[] dataPoint, double[] centroid) {

        double sum = 0;
        for (int i = 0; i < dataPoint.length-1; i++) {
            double num = dataPoint[i];
            sum += Math.pow((centroid[i] - num), 2);
        }
        return sum;
    }


}
