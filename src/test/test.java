package test;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;
/**
 *
 * @author PP
 *
 */
public class Test{
    public static void main(final String[] args) throws Exception {
        /**** Connect to MongoDB ****/
        // Since 2.10.0, uses MongoClient
        MongoClient mongo = new MongoClient("localhost", 27017);
       /**** Get database ****/
        // if database doesn't exists, MongoDB will create it for you
        DB db = mongo.getDB("enron");
        DBCollection table = db.getCollection("demo");
        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("_id", 1);
        DBCursor cursor = table.find(searchQuery).limit(5);
        System.out.println("test");
        System.out.println(JSON.serialize(cursor));

    }

}
