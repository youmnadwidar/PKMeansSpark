import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            System.out.println("Please enter 4 args as follow : <inputFilePath> <outputFilePath> <K for centroids>");
            System.exit(1);
        }
        String input = args[0];
        String output = args[1];
        int k = Integer.parseInt(args[2]);
        PKMeans kMeans = new PKMeans(input, output, k);
        kMeans.runKMeans();
    }

}
