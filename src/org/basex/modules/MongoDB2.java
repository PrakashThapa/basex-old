package org.basex.modules;

import java.net.UnknownHostException;
import java.util.HashMap;

import org.basex.query.QueryException;
import org.basex.query.QueryModule;
import org.basex.query.func.FNJson;
import org.basex.query.func.Function;
import org.basex.query.value.item.Int;
import org.basex.query.value.item.Item;
import org.basex.query.value.item.Str;

import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

/**
 * Mongodb extension of Basex.
 *
 * @author BaseX Team 2005-13, BSD License
 * @author Prakash Thapa
 */
public class MongoDB2 extends QueryModule {
    /**
     * mongoclient instances.
     */
    private HashMap<String, MongoClient> mongoClients =
            new HashMap<String, MongoClient>();
    /**
     * DB instances instances.
     */
    private HashMap<String, DB> dbs =
            new HashMap<String, DB>();
    /**
     * Mongodb Connection from URLstructure: mongodb://root:root@localhost/test.
     * @param url of Mongodb connection
     * @return conncetion DB
     * @throws QueryException
     */
    private DB Connection(final Str url) throws QueryException {
        MongoClientURI uri = new MongoClientURI(url.toJava());
        try {
            MongoClient mongoClient = new MongoClient(uri);
            try {
                DB db = mongoClient.getDB(uri.getDatabase());
                if (uri.getUsername() != null && uri.getPassword() != null) {
                    boolean auth = db.authenticate(uri.getUsername(),
                            uri.getPassword());
                    if (!auth) {
                        throw new QueryException("Invalid "
                    + "username or password");
                    }
                 }
                return db;
                } catch (final MongoException ex) {
                    throw new QueryException(ex);
                 }
            } catch (final MongoException ex) {
                throw new QueryException(ex);
              } catch (UnknownHostException ex) {
                  throw new QueryException(ex);
              }
    }

    /**
	 *  Mongodb connection when provided with host.
	 * port and database separately.
	 * @param host
	 * @param port
	 * @param dbname
	 * @return DB instance
	 * @throws QueryException
	 */
	public Str Connection(final Str host, final Int port, final Str dbname)
	        throws QueryException {
		  String handler = "Client" + mongoClients.size();
		try {
			MongoClient mongoClient = new MongoClient(
			        (String) host.toJava(),
			        (int) port.itr());
			mongoClients.put(handler, mongoClient);
			return Str.get(handler);

		} catch (final MongoException ex) {
			throw new QueryException(ex);
		} catch (UnknownHostException ex) {
			throw new QueryException(ex);
		}
	}

	/**
	 *  DB selection with mongoclient instance.
	 * @param handler MongoClient handler of hashmap
	 * @param dbName Databasename to connect
	 * @return DB handler of hashmap
	 * @throws QueryException
	 */
	public Str selectDb(final Str handler, final Str dbName) throws QueryException {
		String ch = handler.toJava();
		// boolean auth = db.authenticate(username,
		// (char[])password.toCharArray());
		final MongoClient client = mongoClients.get(ch);
		if(client == null)
			throw new QueryException("Unknown MongoDB handler: '" + ch + "'");
		final String dbh = "DB" + dbs.size();
		try {
			DB db = client.getDB(dbName.toJava());
			dbs.put(dbh, db);
			return Str.get(dbh);

		} catch (final MongoException ex) {
			throw new QueryException(ex);
		}
	}

	/**
	 * get DB handler from hashmap.
	 * @param handler hashmap key in Str
	 * @return DB handler
	 * @throws QueryException
	 */
	private DB getDbHandler(final Str handler) throws QueryException {
		final DB db = dbs.get(handler.toJava());
		if(db == null)
			throw new QueryException("Unknown database handler: '" + handler.toJava() + "'");
		return db;
	}
	/**
	 * Collection result(DBCursor) into xml item.
	 * @param result DBCursor
	 * @return Item of Xml
	 * @throws QueryException
	 */
	private Item resultToXml(final DBCursor result) throws QueryException {
		final Str json = Str.get(JSON.serialize(result));
		return new FNJson(null, Function._JSON_PARSE, json).item(context, null);
	}
	/**
	 * Collection object(DBObject) into xml item.
	 * @param object DBObject  (one row result)
	 * @return Item of Xml
	 * @throws QueryException
	 */
	private Item objectToXml(final DBObject object) throws QueryException {
		final Str json = Str.get(JSON.serialize(object));
		return new FNJson(null, Function._JSON_PARSE, json).item(context, null);
	}
	/**
	 * take string as Str parameters and return DBObject of mongodb.
	 * @param string
	 * @return
	 * @throws QueryException
	 */
	private DBObject getDbObjectFromStr(final Str string) throws QueryException {
		try {
			return  (DBObject) JSON.parse(string.toJava());
		} catch (Exception e) {
			throw new QueryException("Invalid input parameters");
		}
	}
	/**
	 * MongoDB find() without any attributes. eg. db.collections.find()
	 * @param handler
	 * @param col
	 * @return result in xml element
	 * @throws QueryException
	 */
	public Item find(final Str url, final Str col) throws QueryException {
		final DB db = Connection(url);
		Item item;
		db.requestStart();
        try {
             item =  new FNJson(null, Function._JSON_PARSE,
                    Str.get(JSON.serialize(
                            db.getCollection(col.toJava()).find()))).item(context, null);
        } finally {
           db.requestDone();
        }
	return item;
	}
	/**
	 * MongoDB find() condtion. eg. db.collections.find({'_id':2})
	 * @param handler Database handler
	 * @param col collection
	 * @param query conditions
	 * @return xml element
	 * @throws QueryException
	
	public Item find(final Str handler, final Str col, final Str query)
	        throws QueryException {
		final DB db = getDbHandler(handler);
		//DBObject queryObj = getDbObjectFromStr(query);
		final DBCursor result = db.getCollection(col.toJava()).find(
		        getDbObjectFromStr(query));
		//return this.toJson(db.getCollection(col).find(obj, f));
		Item item = resultToXml(result);
		return item;
	}
	 */
	/**
	 * just backup function need to remove when cleaning.
	 * @param handler Database handler
	 * @param col collection
	 * @param query conditions
	 * @param field
	 * @return xml elements
	 * @throws QueryException
	 */
	public Item find(final Str handler, final Str col, final Str query, final Str field)
	        throws QueryException {
		final DB db = getDbHandler(handler);
		//DBObject queryObj = (DBObject) JSON.parse(query.toJava());
		//DBObject fieldObj = (DBObject) JSON.parse(field.toJava());
		final DBCursor result = db.getCollection(col.toJava()).find(
		        getDbObjectFromStr(query), getDbObjectFromStr(field));
		//return this.toJson(db.getCollection(col).find(obj, f));
		Item item = resultToXml(result);
		return item;
	}
	/**
	 * Mongodb's findOne() function.
	 * @param handler
	 * @param col
	 * @return
	 * @throws QueryException
	 */
	public Item findOne(final Str handler, final Str col)throws QueryException {
		final DBObject oneresult =  getDbHandler(handler).getCollection(
		        col.toJava()).findOne();
		Item item = objectToXml(oneresult);
		return item;
	}
	/**
	 * Mongodb's findOne({'_id':2}) function with query.
	 * @param handler
	 * @param col
	 * @param query
	 * @return
	 * @throws QueryException
	 */
	public Item findOne(final Str handler, final Str col, final Str query)
	        throws QueryException {
		Item item = objectToXml(getDbHandler(handler).getCollection(col.toJava()).findOne(
		        getDbObjectFromStr(query)));
		return item;
	}
	/**
	 * findOne with query projection and fields.
	 * @param handler
	 * @param col
	 * @param query
	 * @param field
	 * @return
	 * @throws QueryException
	 */
	public Item findOne(final Str handler, final Str col, final Str query, final Str field)
	        throws QueryException {
		final DBObject oneresult =  getDbHandler(handler).getCollection(col.toJava()).findOne(
		        getDbObjectFromStr(query), getDbObjectFromStr(field));
		Item item = objectToXml(oneresult);
		return item;
	}
	/**
	 * insert data.
	 * @param handler DB Handler
	 * @param col Collection name
	 * @param insertString string to insert in json formart
	 * @throws QueryException
	 */
	public void insert(final Str handler, final Str col, final Str insertString)
	        throws QueryException {
//		final DB db = getDbHandler(handler);
//		DBObject obj = (DBObject) JSON.parse(insertString.toJava());
//		db.getCollection(col.toJava()).insert(obj);
		getDbHandler(handler).getCollection(
		        col.toJava()).insert(getDbObjectFromStr(insertString));
	}
	/**
	 * need to complete.
	 * @param handler
	 * @param col
	 * @param insertString
	 * @throws QueryException
	 */
	public void update(final Str handler, final Str col, final Str insertString)
	        throws QueryException {
		getDbHandler(handler).getCollection(col.toJava()).insert(
		        getDbObjectFromStr(insertString));
	}
	/**
	 * Mongodb Save function.
	 * @param handler DB handler
	 * @param col collection name
	 * @param insertString string in Str  to save
	 * @throws QueryException
	 */
	public void save(final Str handler, final Str col, final Str saveStr)
	        throws QueryException {
		getDbHandler(handler).getCollection(col.toJava()).save(
		        getDbObjectFromStr(saveStr));
	}
	/**
	 * Mongodb remove().
	 * @param handler
	 * @param col
	 * @param removeStr
	 * @throws QueryException
	 */
	public void remove(final Str handler, final Str col, final Str removeStr)
            throws QueryException {
	    try {
	        getDbHandler(handler).getCollection(col.toJava()).remove(
	                getDbObjectFromStr(removeStr));
          } catch (final MongoException ex) {
            throw new QueryException(ex);
          }
    }
	/**
	 * convert json to xml.
	 * @param json
	 * @return Items in xml  format
	 * @throws QueryException
	 */
	public Item jsonToXml(final Str json) throws QueryException {
		return new FNJson(null, Function._JSON_PARSE, json).item(context, null);
	}
	/**
	 * take DB handler as parameter and get MongoClient and then close it.
	 * @param handler DB handler
	 * @throws QueryException
	 */
	public void close(final Str handler) throws QueryException {
		String ch = handler.toJava();
		final MongoClient client = (MongoClient) getDbHandler(handler).getMongo();
		if(client == null)
			throw new QueryException("Unknown MongoDB handler: '" + ch + "'");
		client.close();
//		final MongoClient client = connections.get(handler);
//		if(client == null)
//			throw new QueryException("Unknown MongoDB handler: '" + ch + "'");
//		client.close();
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
	 * for testing.
	 */
	public void getSize() {
	    System.out.println(mongoClients.size() + " db-> " + dbs.size());
	}
	/**
	 * test find with each connection.
	 * 
	 */
	public Item testFind(final Str url, final Str col) throws QueryException {
        final Str handler = testConnection(url);
        try {
            DB db = getDbHandler(handler);
            final DBCursor result  = db.getCollection(col.toJava()).find();
            return resultToXml(result);
            //return Str.get(JSON.serialize(result));
            //return Str.get(JSON.serialize(
            //        getDbHandler(handler).getCollection(col.toJava()).find()));
        } finally {
            close(handler);
        }
    }
	
	public Item test2Find(final Str url, final Str col) throws QueryException {
        DB db = Connection(url);
        db.requestStart();
        Str s;
        try {
            DBCursor result  = db.getCollection(col.toJava()).find();
            s = Str.get(JSON.serialize(result));
            
        } finally {
           db.requestDone();
        }
        return s;
        //return Str.get(JSON.serialize(result));
    }
	/**
	 * test connection
	 */
	 private Str testConnection(final Str url) throws QueryException {
	     
	     MongoClientURI uri = new MongoClientURI(url.toJava());
	        String handler = "Client" + mongoClients.size();
	        try {
	            MongoClient mongoClient = new MongoClient(uri);
	            mongoClients.put(handler, mongoClient);
	            final String dbh = "DB" + dbs.size();
	            try {
	                DB db = mongoClient.getDB(uri.getDatabase());
	                if (uri.getUsername() != null && uri.getPassword() != null) {
	                    boolean auth = db.authenticate(uri.getUsername(),
	                            uri.getPassword());
	                    if (!auth) {
	                        throw new QueryException("Invalid "
	                    + "username or password");
	                    }
	                 }
	                dbs.put(dbh, db);
	                return Str.get(dbh);
	                } catch (final MongoException ex) {
	                    throw new QueryException(ex);
	                 }
	            } catch (final MongoException ex) {
	                throw new QueryException(ex);
	              } catch (UnknownHostException ex) {
	                  throw new QueryException(ex);
	              }
	        }
	 private void testClose(final DB db) throws QueryException {
	        final MongoClient client = (MongoClient)db.getMongo();
	        if(client == null)
	            throw new QueryException("Unknown MongoDB handler: '" + 
	                    db.toString() + "'");
	        client.close();
	    }
}
