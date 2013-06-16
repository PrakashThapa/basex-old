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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;

import com.mongodb.util.JSON;

public class MongoDB extends QueryModule {
	private HashMap<String, MongoClient> connections =
			new HashMap<String, MongoClient>();
	private HashMap<String, DB> dbs =
			new HashMap<String, DB>();
	private DB db = null;
	private MongoClient mongoClient = null;
	
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

	/**
	 * 
	 * @param url of Mongodb connection
	 * @return conncetion handler of Mongodb
	 * @throws QueryException
	 */
	public Str Connection(final String url) throws QueryException {
		
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

	public Item find(final Str handler, final Str col) throws QueryException {
		String ch = handler.toJava();
		final DB db = dbs.get(ch);
		if(db == null)
			throw new QueryException("Unknown database handler: '" + ch + "'");
		final DBCollection coll = db.getCollection(col.toJava());
		final DBCursor result = coll.find();
		//return  jsonToXml(Str.get(JSON.serialize(result)));
		return resultToXml(result);
		
	}
	
	public Item find(final Str handler,String col, String json, String field)throws QueryException {
		String ch = handler.toJava();
		final DB db = dbs.get(ch);
		if(db == null)
			throw new QueryException("Unknown database handler: '" + ch + "'");
		DBObject obj = (DBObject) JSON.parse(json);
		DBObject f = (DBObject) JSON.parse(field);
		//return this.toJson(db.getCollection(col).find(obj, f));
		return null;
	}
	
	/**
	 * 
	 * @param result DBCursor 
	 * @return Item of Xml 
	 * @throws QueryException
	 */
	private Item resultToXml(final DBCursor result) throws QueryException {
		final Str json = Str.get(JSON.serialize(result));
		return new FNJson(null, Function._JSON_PARSE, json).item(context, null);
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

	
	
	public void cls(final Str handler) throws QueryException {
		String ch = handler.toJava();

		final MongoClient client = connections.get(handler);
		if(client == null)
			throw new QueryException("Unknown MongoDB handler: '" + ch + "'");
		client.close();
	}
	
	public void insertCollection(String col, BasicDBObject bdo) {
		db.getCollection(col).insert(bdo);
	}

	/**
	 * 
	 * @param col
	 * @param bdo
	 * @return
	 */
	public DBCursor getCollection(String col, BasicDBObject bdo) {
		return db.getCollection(col).find(bdo);
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
	 * @param col
	 * @return DBCursor Object(without json convert)
	 */
	public DBCursor findNormal(String col) {
		return db.getCollection(col).find();
	}

	/**
	 * 
	 * @param col
	 * @return json format of collections in string
	 */
	public String find(String col) {
		try {
			final DBCollection coll = db.getCollection(col);
			final DBCursor result = coll.find();
			String string = this.toJson(result);
			return string;
		} catch (NullPointerException e) {
			e.printStackTrace();
			return "error";
		}

	}


	

	/**
	 * Convert the DBCursor Object into JSON. easy to implement
	 * 
	 * @param cursor
	 * @return json format data of DBCursor
	 */
	private String toJson(DBCursor cursor) {
		String s = JSON.serialize(cursor);
		System.out.println(s);
		return s;
	}

	/**
	 * 
	 * @param dbName
	 *            Name of database that wanted to be drop
	 */
	public void drop(String dbName) {
		mongoClient.dropDatabase(dbName);
	}

	/**
	 * close mongodb instance
	 */
	public void close() {
		 mongoClient.close();
	}

}
