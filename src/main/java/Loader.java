import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ArrayBlockingQueue;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

public class Loader extends Thread {
    ArrayBlockingQueue<String> readBuffer;
    String path;
    public Loader(ArrayBlockingQueue<String> readBuffer, String path) {
        this.readBuffer=readBuffer;
        this.path=path;
    }

    @Override
    public void run() {

            File file= new File(path);
            InputStreamReader reader = null;
            try {
                FileInputStream fis = new FileInputStream(file);
                reader = new InputStreamReader(new BZip2CompressorInputStream(fis));
            } catch (IOException e){
                e.printStackTrace();
            }
        BufferedReader rd = new BufferedReader(reader);

            int readCount=0;
            int cleanThreshold=6000;

        String line = null;
        try {
            while ((line = rd.readLine()) != null) {
                if (line.equals("[") || line.equals("]")) continue;
                readBuffer.put(line);
                if (readCount++ == cleanThreshold) {
                    cleanThreshold += 3000;
                    System.out.println("Cleaning " + readCount);
                    System.gc();
                }
            }
        } catch (InterruptedException | IOException e) {
            e.printStackTrace();
        }

    }
}
