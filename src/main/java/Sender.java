import com.mongodb.client.MongoCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;


public class Sender implements Runnable {

    MongoCollection<Document> collection;
    MongoCollection<Document> bodiesCollection;
    MongoCollection<Document> stationCollection;
    List<Document> insertQueue = new ArrayList<Document>();
    HashMap<String, List<Document>> insertQueueExtra = new HashMap<>();
    ArrayBlockingQueue<String> readBuffer;
    Map<Type,MongoCollection<Document>> pool;

    public Sender( Map<Type,MongoCollection<Document>> pool, ArrayBlockingQueue<String> readBuffer) {
 /*       this.collection = pool.get(0);
        this.bodiesCollection = pool.get(1);
        this.stationCollection = pool.get(2);*/
        this.readBuffer = readBuffer;
        insertQueueExtra.put(Type.BODIES.getVal(), new ArrayList<Document>());
        insertQueueExtra.put(Type.STATIONS.getVal(), new ArrayList<Document>());
    }

    @Override
    public void run() {
        JSONObject json = null;

        while (true) {
            try {
                json = new JSONObject(readBuffer.take());
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Map jsonMap = json.toMap();
            if (!((JSONArray) json.get(Type.BODIES.getVal())).isEmpty()) {
                System.out.println("Not empty bodies section detected");
                splitEntry(jsonMap, Type.BODIES.getVal());
            }
            if (!((JSONArray) json.get(Type.STATIONS.getVal())).isEmpty()) {
                System.out.println("Not empty station section detected");
                splitEntry(jsonMap, Type.STATIONS.getVal());
            }

            Document doc = new Document();
            doc.putAll(jsonMap);
            insertQueue.add(doc);
            if (insertQueue.size() > 1000) {
                System.out.println("wrote: " + this.toString() + " 1000 docs");
                pool.get(Type.SYSTEM).insertMany(insertQueue);
                insertQueueExtra.entrySet().forEach(entry->{if(!entry.getValue().isEmpty()){
                    pool.get(Type.valueOf(entry.getKey())).insertMany(entry.getValue());
                    entry.getValue().clear();
                }});
                insertQueue.clear();
                System.gc();
            }
        }

    }

    private void splitEntry(Map jsonMap, String fieldName) {
        ArrayList<HashMap<String, Object>> bodiesArray = (ArrayList<HashMap<String, Object>>) jsonMap.get(fieldName);
        ArrayList bodiesReference = new ArrayList();
        System.out.printf("number of %s : %d \n", fieldName, bodiesArray.size());
        bodiesArray.forEach(bodiesEntryMap -> {
            bodiesEntryMap.remove("updateTime");
            ObjectId id = new ObjectId();
            bodiesEntryMap.put("_id", id);
            bodiesReference.add(id);
            insertQueueExtra.get(fieldName).add(new Document(bodiesEntryMap));
        });
        jsonMap.put(fieldName, bodiesReference);
    }
}
