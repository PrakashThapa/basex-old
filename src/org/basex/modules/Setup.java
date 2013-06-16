package org.basex.modules;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;
/**
 * 
 * @author Prakash Thapa
 *
 */
public class Setup  {
    private static  Mongo mongo	= null;
    protected DB db = null;
    @SuppressWarnings("deprecation")
	public DB Connection(final String dname) {
    	
    	 if (mongo == null) {
             try {
                     mongo = new Mongo();
                     db = mongo.getDB(dname);
                 } catch (UnknownHostException e) {
                     mongo = null;
                     e.printStackTrace();
                 } catch (MongoException e) {
                     mongo = null;
                     e.printStackTrace();
                 }
             }
    	 //return mongo;
    	
    	
//    	 try {
//             mongo = new Mongo();
//              db = mongo.getDB(dname);
//         } catch (Exception e) {
//             throw new MongoException("test Error");
//             
//         }
    	 return db;
    }
    @SuppressWarnings("deprecation")
	public void Connection(String host, int port, String dname) throws Exception {
    	
        try {
            mongo = new Mongo(host, port);
            db = mongo.getDB(dname);
        } catch (Exception e) {
            throw new MongoException("test Error");
            
        }
    }
    /**
     * 
     * @return
     */
    protected  Mongo getMongo() {
        return mongo;
    }
    /**
     * 
     * @return
     */
    protected DB getDb() {
    	return db;
    }
   /**
    * 
    * @param col
    * @param bdo
    */
   public void insertCollection(String col, BasicDBObject bdo) {
	   db.getCollection(col).insert(bdo);
   }
   /**
    * 
    * @param col
    * @param bdo
    * @return
    */
   public DBCursor getCollection(String col,BasicDBObject bdo) {
	  return  db.getCollection(col).find(bdo);
   }
   /**
    * 
    * @param col
    * @param json
    */
   public void insertJson(String col, String json) {
	   DBObject obj = (DBObject) JSON.parse(json);
	  db.getCollection(col).insert(obj);
   }
   /**
    * 
    * @param json
    */
   public void insertJson(String json) {
	   DBObject obj = (DBObject) JSON.parse(json);
	  //db.getCollection(col).insert(obj);
   }
   /**
    * 
    * @param col
    * @return DBCursor Object(without json convert)
    */
   public DBCursor findNormal(String col) {
		 return db.getCollection(col).find();
	}
   /**
    * 
    * @param col
    * @return json format of collections
    */
   public String find(String col) {
	   return this.toJson(db.getCollection(col).find());
   }
   
   /**
    * 
    * @param col Collection Name
    * @param json Json string
    * @return
    */
   public String find(String col,String json) {
	   DBObject obj = (DBObject) JSON.parse(json);
	   return this.toJson(db.getCollection(col).find(obj));
   }
   /**
    * Convert the DBCursor Object into JSON. easy to implement
    * @param cursor
    * @return json format data of DBCursor
    */
   public String toJson(DBCursor cursor) {
	   return JSON.serialize(cursor);
   }
   
   /**
    * close mongodb instance
    */
   public void close() {
	   mongo.close();
   }
}