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
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;


import com.mongodb.util.JSON;

/**
 * Functions on Mongodb databases.
 *
 * @author BaseX Team 2005-13, BSD License
 * @author Prakash Thapa
 */
public class MongoDB extends QueryModule {
	private HashMap<String, MongoClient> connections =
			new HashMap<String, MongoClient>();
	private HashMap<String, DB> dbs =
			new HashMap<String, DB>();

	/**
	 * 
	 * @param url of Mongodb connection
	 * @return conncetion handler of Mongodb
	 * @throws QueryException
	 */
	public Str Connection(final Str url) throws QueryException {
		
		MongoClientURI uri = new MongoClientURI(url.toJava());
		String handler = "Client" + connections.size();
		try {
			MongoClient mongoClient = new MongoClient(uri);
			connections.put(handler, mongoClient);
			//select database
			final String dbh = "DB" + dbs.size();
			try {
				DB db = mongoClient.getDB(uri.getDatabase());
				if(uri.getUsername()!=null && uri.getPassword()!=null) {
					boolean auth = db.authenticate(uri.getUsername(), uri.getPassword());
						if(!auth) 
							throw new QueryException("Invalid username or password");
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
	/**
	 *  Mongodb  connection when provided with host port and db separately. 	
	 * @param host
	 * @param port
	 * @param dbname
	 * @return DB instance
	 * @throws QueryException
	 */
	public Str Connection(final Str host, final Int port, final Str dbname) throws QueryException {
		  String handler = "Client" + connections.size();
		try {
			MongoClient mongoClient = new MongoClient((String)host.toJava(),(int)port.itr());//
			connections.put(handler, mongoClient);
			return Str.get(handler);

		} catch (final MongoException ex) {
			throw new QueryException(ex);
		} catch (UnknownHostException ex) {
			throw new QueryException(ex);
		}
	}

	
	public Str cnnctn() throws QueryException {
		String handler = "Client" + connections.size();
		try {
			MongoClient mongoClient = new MongoClient();
			connections.put(handler, mongoClient);
			return Str.get(handler);

		} catch (final MongoException ex) {
			throw new QueryException(ex);
		} catch (UnknownHostException ex) {
			throw new QueryException(ex);
		}
	}

	public Str selectDb(final Str handler, final Str dbName) throws QueryException {
		String ch = handler.toJava();
		// boolean auth = db.authenticate(username,
		// (char[])password.toCharArray());
		final MongoClient client = connections.get(ch);
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

	private DB getDbHandler(final Str handler) throws QueryException {
		String ch = handler.toJava();
		final DB db = dbs.get(ch);
		if(db == null)
			throw new QueryException("Unknown database handler: '" + ch + "'");
		return db;
	}
	
	/**
	 * Collection result(DBCursor) into xml item
	 * @param result DBCursor 
	 * @return Item of Xml 
	 * @throws QueryException
	 */
	private Item resultToXml(final DBCursor result) throws QueryException {
		final Str json = Str.get(JSON.serialize(result));
		return new FNJson(null, Function._JSON_PARSE, json).item(context, null);
	}
	/**
	 * Collection object(DBObject) into xml item
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
	public Item find(final Str handler, final Str col) throws QueryException {
		
		final DB db = getDbHandler(handler);
		final DBCursor result  = db.getCollection(col.toJava()).find();
		//return  jsonToXml(Str.get(JSON.serialize(result)));
		Item item =resultToXml(result); 
		close(handler);
		return item;
		
	}
	
	/**
	 * MongoDB find() condtion. eg. db.collections.find({'_id':2})
	 * @param handler Database handler
	 * @param col collection
	 * @param query conditions
	 * @return xml element
	 * @throws QueryException
	 */
	public Item find(final Str handler,Str col, Str query)throws QueryException {
		
		final DB db = getDbHandler(handler);
		//DBObject queryObj = getDbObjectFromStr(query);
		final DBCursor result = db.getCollection(col.toJava()).find(getDbObjectFromStr(query));
		//return this.toJson(db.getCollection(col).find(obj, f));
		Item item =resultToXml(result); 
		close(handler);
		return item;
	}
	
	/**
	 * MongoDB find() with query and projection. eg. db.collections.find({'_id':2},{'name':1})
	 * @param handler Database handler
	 * @param col collection
	 * @param query conditions
	 * @param field
	 * @return xml elements
	 * @throws QueryException
	 */
	public Item find(final Str handler,Str col, Str query, Str field)throws QueryException {
		
		final DB db = getDbHandler(handler);
		//DBObject queryObj = (DBObject) JSON.parse(query.toJava());
		//DBObject fieldObj = (DBObject) JSON.parse(field.toJava());
		final DBCursor result = db.getCollection(col.toJava()).find(getDbObjectFromStr(query),getDbObjectFromStr(field));
		//return this.toJson(db.getCollection(col).find(obj, f));
		Item item =resultToXml(result); 
		close(handler);
		return item;		
	}
	
	/**
	 * 
	 * @param handler
	 * @param col
	 * @return
	 * @throws QueryException
	 */
	public Item findOne(final Str handler,Str col)throws QueryException {
		
		final DBObject oneresult =  getDbHandler(handler).getCollection(col.toJava()).findOne();
		Item item =objectToXml(oneresult); 
		close(handler);
		return item;		
	}
	
	/**
	 * 
	 * @param handler
	 * @param col
	 * @param query
	 * @return
	 * @throws QueryException
	 */
	public Item findOne(final Str handler,Str col, Str query)throws QueryException {
		
		Item item =objectToXml(getDbHandler(handler).getCollection(col.toJava()).findOne(getDbObjectFromStr(query))); 
		close(handler);
		return item;		
	}
	/**
	 * 
	 * @param handler
	 * @param col
	 * @param query
	 * @param field
	 * @return
	 * @throws QueryException
	 */
	public Item findOne(final Str handler,Str col, Str query, Str field)throws QueryException {
		final DBObject oneresult =  getDbHandler(handler).getCollection(col.toJava()).findOne(getDbObjectFromStr(query),getDbObjectFromStr(field));
		Item item =objectToXml(oneresult); 
		close(handler);
		return item;		
	}
	/**
	 * 
	 * @param handler DB Handler 
	 * @param col Collection name
	 * @param insertString string to insert in json formart
	 * @throws QueryException
	 */
	public void insert(final Str handler, final Str col, Str insertString) throws QueryException {
//		final DB db = getDbHandler(handler);
//		DBObject obj = (DBObject) JSON.parse(insertString.toJava());
//		db.getCollection(col.toJava()).insert(obj);
		getDbHandler(handler).getCollection(col.toJava()).insert(getDbObjectFromStr(insertString));
	}
	
	public void update(final Str handler, Str col, Str insertString) throws QueryException {
		getDbHandler(handler).getCollection(col.toJava()).insert(getDbObjectFromStr(insertString));
	}
	
	/**
	 * Mongodb Save function
	 * @param handler DB handler
	 * @param col collection name
	 * @param insertString string in Str  to save
	 * @throws QueryException
	 */
	public void save(final Str handler,final Str col, final Str insertString) throws QueryException {
		getDbHandler(handler).getCollection(col.toJava()).save(getDbObjectFromStr(insertString));
	}
	
	
	/**
	 * 
	 * @param json
	 * @return Items in xml  format
	 * @throws QueryException
	 */
	public Item jsonToXml(Str json) throws QueryException {
		return new FNJson(null, Function._JSON_PARSE, json).item(context, null);
	}
	
	public Str fnd(final Str handler, final Str col) throws QueryException {
		String ch = handler.toJava();
		final DB db = dbs.get(ch);
		if(db == null)
			throw new QueryException("Unknown database handler: '" + ch + "'");

		final DBCollection coll = db.getCollection(col.toJava());
		final DBCursor result = coll.find();
		return Str.get(toJson(result));
	}

	
	/**
	 * take DB handler as parameter and get MongoClient and then close it
	 * @param handler DB handler 
	 * @throws QueryException
	 */
	private void close(final Str handler) throws QueryException {
		String ch = handler.toJava();
//		
//		final MongoClient client = connections.get(handler);
//		if(client == null)
//			throw new QueryException("Unknown MongoDB handler: '" + ch + "'");
//		client.close();
		
		final MongoClient client = (MongoClient)getDbHandler(handler).getMongo();
		if(client == null)
			throw new QueryException("Unknown MongoDB handler: '" + ch + "'");
		client.close();
	}
	
	/**
	 * Convert the DBCursor Object into JSON. easy to implement
	 * 
	 * @param cursor
	 * @return json format data of DBCursor
	 */
	private String toJson(DBCursor cursor) {
		return JSON.serialize(cursor);
	}

	
	
}
