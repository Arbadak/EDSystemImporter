import com.mongodb.client.MongoCollection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONArray;
import org.json.JSONObject;


public class Sender implements Runnable {

    List<Document> insertQueue = new ArrayList<>();
    HashMap<String, List<Document>> insertQueueExtra = new HashMap<>();
    ArrayBlockingQueue<String> readBuffer;
    Map<SectionType, MongoCollection<Document>> pool;

    public Sender(Map<SectionType, MongoCollection<Document>> pool, ArrayBlockingQueue<String> readBuffer) {
        this.pool = pool;
        this.readBuffer = readBuffer;
        insertQueueExtra.put(SectionType.BODIES.getValue(), new ArrayList<>());
        insertQueueExtra.put(SectionType.STATIONS.getValue(), new ArrayList<>());
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
            if (!((JSONArray) json.get(SectionType.BODIES.getValue())).isEmpty()) {
                System.out.println("Not empty bodies section detected");
                transferEntryToSeparateCollection(jsonMap, SectionType.BODIES.getValue());
            }
            if (!((JSONArray) json.get(SectionType.STATIONS.getValue())).isEmpty()) {
                System.out.println("Not empty station section detected");
                transferEntryToSeparateCollection(jsonMap, SectionType.STATIONS.getValue());
            }

            Document doc = new Document();
            doc.putAll(jsonMap);
            insertQueue.add(doc);
            if (insertQueue.size() > 1000) {
                System.out.println("wrote: " + this.toString() + " 1000 docs");
                pool.get(SectionType.SYSTEM).insertMany(insertQueue);
                insertQueueExtra.entrySet().forEach(entry -> {
                    if (!entry.getValue().isEmpty()) {
                        pool.get(SectionType.valueOf(entry.getKey().toUpperCase())).insertMany(entry.getValue());
                        entry.getValue().clear();
                    }
                });
                insertQueue.clear();
                System.gc();
            }
        }

    }

    private void transferEntryToSeparateCollection(Map<String, Object> jsonMap, String fieldName) {
        ArrayList<HashMap<String, Object>> sectionArray = (ArrayList<HashMap<String, Object>>) jsonMap.get(fieldName);
        ArrayList<ObjectId> sectionReference = new ArrayList<>();
        System.out.printf("number of %s : %d \n", fieldName, sectionArray.size());
        sectionArray.forEach(sectionEntryMap -> {
            sectionEntryMap.remove("updateTime");
            ObjectId id = new ObjectId();
            sectionEntryMap.put("_id", id);
            sectionReference.add(id);
            insertQueueExtra.get(fieldName).add(new Document(sectionEntryMap));
        });
        jsonMap.put(fieldName, sectionReference);
    }
}
