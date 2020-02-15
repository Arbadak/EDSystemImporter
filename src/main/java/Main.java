import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.bson.Document;

public class Main {
static final String FILE="c:\\Downloads\\galaxy.json.bz2";
static final String URL="https://downloads.spansh.co.uk/galaxy.json.bz2";

    public static void main(String[] args) throws Exception {
   /*     JsonReader reader = new JsonReader();
//        reader.readJsonFromUrl(URL);
        reader.readJsonFromUrl("c:\\Downloads\\galaxy.json.bz2");*/

        ExecutorService executorService= new ScheduledThreadPoolExecutor(5);
        ArrayBlockingQueue<String> buffer=new ArrayBlockingQueue<>(3000);
        MongoDatabase database= new MongoClient().getDatabase("elite");
        Map<Type,MongoCollection<Document>> pool = new HashMap<>();
        pool.put(Type.SYSTEM,database.getCollection(Type.SYSTEM.getVal()));
        pool.put(Type.BODIES,database.getCollection(Type.BODIES.getVal()));
        pool.put(Type.STATIONS,database.getCollection(Type.STATIONS.getVal()));

        executorService.submit(new Loader(buffer,FILE));
        executorService.submit(new Sender(pool, buffer));
        executorService.submit(new Sender(pool, buffer));
        executorService.submit(new Sender(pool, buffer));
        executorService.submit(new Sender(pool, buffer));

    }
}
