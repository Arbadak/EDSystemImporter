import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.bson.Document;
import org.json.JSONObject;

@Deprecated
public class JsonReader {
    //List jsonObj = new ArrayList();
    long counter=0;
    long thresh=5000;

    public void readJsonFromUrl(String url) throws Exception {
        MongoClient client=new MongoClient();
        MongoDatabase database = client.getDatabase("edsm_dump");
        MongoCollection<Document> collection = database.getCollection("spansh");
        //InputStreamReader gzipReader = getReader(url);
        InputStreamReader gzipReader = getFileReader(url);

        try {
            BufferedReader rd = new BufferedReader(gzipReader);
            String line = null;
            List<Document>insertQueue= new ArrayList<Document>();
            while ((line = rd.readLine()) != null) {
                if (line.equals("[") || line.equals("]")) continue;

                JSONObject json = new JSONObject(line);

                Map jsonMap = json.toMap();
                Document doc = new Document();
                doc.putAll(jsonMap);
                insertQueue.add(doc);
                if (counter++==thresh){
                    thresh +=30000;
                    System.out.println("wrote: "+counter);
                    collection.insertMany(insertQueue);
                    insertQueue.clear();
                    System.gc();
                }

            }

        } finally {
            gzipReader.close();
        }
    }


    public InputStreamReader getReader(String urlstrin) throws Exception {
        URL url = new URL(urlstrin);
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestProperty("Accept-Encoding", "gzip");
        System.out.println("Length : " + con.getContentLength());

        InputStreamReader reader = null;
//        if ("gzip".equals(con.getContentEncoding())) {
        if (true) {
//            reader = new InputStreamReader(new GZIPInputStream(con.getInputStream()), Charset.forName("UTF-8"));
            reader = new InputStreamReader(new BZip2CompressorInputStream(con.getInputStream()), Charset.forName("UTF-8"));
        } else {
            reader = new InputStreamReader(con.getInputStream());
        }

        return reader;
    }

    private InputStreamReader getFileReader(String path){
        File file= new File(path);
        InputStreamReader reader = null;
        try {
            FileInputStream fis = new FileInputStream(file);
            reader = new InputStreamReader(new BZip2CompressorInputStream(fis));
        }
        catch (FileNotFoundException e){
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return reader;
    }
}
