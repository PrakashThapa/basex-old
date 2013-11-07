package org.basex.modules;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import org.basex.query.QueryException;
import org.basex.query.func.FNJson;
import org.basex.query.func.FuncOptions;
import org.basex.query.func.Function;
import org.basex.query.value.Value;
import org.basex.query.value.item.Int;
import org.basex.query.value.item.Item;
import org.basex.query.value.item.QNm;
import org.basex.query.value.item.Str;
import org.basex.query.value.map.Map;
import org.basex.query.value.type.SeqType;

import com.mongodb.AggregationOutput;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;

/**
 * Mongodb extension of Basex.
 *
 * @author BaseX Team 2005-13, BSD License
 * @author Prakash Thapa
 */
public class MongoDB extends Nosql {
    public MongoDB() {
        super(Q_MONGODB);
    }
    /** URL of this module. */
    private static final String MONGO_URL = "http://basex.org/modules/mongodb";
    /** QName of MongoDB options. */
    private static final QNm Q_MONGODB = QNm.get("mongodb", "options",
            MONGO_URL);
    /** mongoclient instances. */
    private HashMap<String, MongoClient> mongoClients =
            new HashMap<String, MongoClient>();
    /** DB instances instances. */
    private HashMap<String, DB> dbs = new HashMap<String, DB>();
    /** Mongo Options instances. */
    private HashMap<String, NosqlOptions> mongopts = new HashMap<String, NosqlOptions>();
    /**
     * Mongodb Connection from URLstructure: mongodb://root:root@localhost/test.
     * @param url of Mongodb connection
     * @return conncetion DB connection handler of Mongodb
     * @throws QueryException
     */
    public Str connection(final Str url) throws QueryException {
        return connection(url, null);
    }
    /**
     * Mongodb connection with options.
     * @param url
     * @param options
     * @return
     * @throws QueryException
     */
    public Str connection(final Str url, final Map options)
            throws QueryException {
        final NosqlOptions opts = new NosqlOptions();
        if(options != null) {
            new FuncOptions(Q_MONGODB, null).parse(options, opts);
        }
        MongoClientURI uri = new MongoClientURI(url.toJava());
        String handler = "mongoClient" + mongoClients.size();
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
                if(options != null) {
                    mongopts.put(dbh, opts);
                }
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
     * Mongodb connection when provided with host port and database separately.
     * @param host host name
     * @param port port
     * @param dbname name of databases
     * @return DB instance
     * @throws QueryException
     */
    public Str connection(final Str host, final Int port, final Str dbname)
            throws QueryException {
          String handler = "Client" + mongoClients.size();
          try {
            MongoClient mongoClient = new MongoClient((String) host.toJava(),
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
     * DB selection with mongoclient instance.
     * @param handler MongoClient handler of hashmap
     * @param dbName Databasename to connect
     * @return DB handler of hashmap
     * @throws QueryException
     */
    public Str selectDb(final Str handler, final Str dbName)
            throws QueryException {
        String ch = handler.toJava();
        final MongoClient client = mongoClients.get(ch);
        if(client == null)
            throw new QueryException("Unknown MongoDB handler: '" + ch + "'");
        final String dbh = "DB" + dbs.size();
        try {
            DB db = client.getDB(dbName.toJava());
            dbs.put(dbh, db);
            return Str.get(dbh);
        } catch (final Exception ex) {
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
            throw new QueryException("Unknown database handler: '" +
        handler.toJava() + "'");
        return db;
      }
    /**
     * get Mongooptions from particular db handler.
     * @param handler
     * @return MongoOptions
     */
    private NosqlOptions getMongoOption(final Str handler) {
        NosqlOptions opt = mongopts.get(handler.toJava());
        if(opt != null)
            return opt;
        else
            return null;
    }
    private Item returnResult(final Str handler, final Str json)
            throws Exception {
        NosqlOptions opt =   getMongoOption(handler);
        if(json != null) {
                if(opt != null) {
                    return finalResult(json, opt);
                } else {
                    return finalResult(json, null);
                }
        } else {
          return  null;
        }
    }
    /**
     * Collection result(DBCursor) into xml item.
     * @param result DBCursor
     * @return Item of Xml
     * @throws QueryException
     */
    private Item resultToXml(final Str handler, final DBCursor result)
            throws QueryException {
        if(result != null) {
            try {
                final Str json = Str.get(JSON.serialize(result));
//                return new FNJson(null, null, Function._JSON_PARSE, json).
//                        item(queryContext, null);
                return returnResult(handler, json);
            } catch (final Exception ex) {
                throw new QueryException(ex);
            }
        } else {
          return  null;
        }
    }
    /**
     * Collection object(DBObject) into xml item.
     * @param object DBObject  (one row result)
     * @return Item of Xml
     * @throws QueryException
     */
    private Item objectToXml(final Str handler, final DBObject object)
            throws QueryException {
        if(object != null) {
            try {
                final Str json = Str.get(JSON.serialize(object));
                return returnResult(handler, json);

            } catch (final Exception ex) {
                throw new QueryException(ex);
            }
        } else {
          return  null;
        }
    }
    /**
     * take string as Str parameters and return DBObject of mongodb.
     * @param string
     * @return
     * @throws QueryException
     */
    private DBObject getDbObjectFromStr(final Item item) throws QueryException {
        final String string = itemToString(item);
        try {
          return  (DBObject) JSON.parse(itemToString(item));
    } catch (JSONParseException e) {
      throw new QueryException("Invalid JSON syntax: " + string);
        }
    }
    /**
     * Return all the collections in current database.
     * @param string
     * @return result in xml element
     * @throws QueryException
     */
    public Item collections(final Str handler) throws QueryException {
        final DB db = getDbHandler(handler);
      Set<String> col = db.getCollectionNames();
      BasicDBObject collection = new BasicDBObject("collections", col);
      try {
        return objectToXml(handler, (DBObject) collection);
      } catch (JSONParseException e) {
          throw new QueryException("Invalid JSON syntax: " + col);
         }
    }
    /**
     * MongoDB find() without any attributes. eg. db.collections.find()
     * @param handler Database handler
     * @param col Collection name
     * @return result in xml element
     * @throws QueryException
     */
    public Item find(final Str handler, final Item col) throws QueryException {
        return find(handler, col, null, null, null);
    }
    /**
     * MongoDB find() condtion. eg. db.collections.find({'_id':2})
     * @param handler Database handler
     * @param col collection
     * @param query conditions
     * @return xml element
     * @throws QueryException
     */
    public Item find(final Str handler, final Item col, final Item query)
            throws QueryException {
      return find(handler, col, query, null, null);
    }
    /**
     * MongoDB find() condition and option.
     * @param handler Database handler
     * @param col collection
     * @param query conditions
     * @param field  projection on Mongodb
     * @return xml elements
     * @throws QueryException
     */
    public Item find(final Str handler, final Str col, final Str query,
             final Map options) throws QueryException {
         return find(handler, col, query, options, null);
       }
    /**
     * Mongodb Find method real implementation.
     * @param handler Database handler
     * @param col collection
     * @param query conditions
     * @param field  projection on Mongodb
     * @return xml elements
     * @throws QueryException
     */
    public Item find(final Str handler, final Item col, final Item query,
            final Map options, final Item projection) throws QueryException {

          final DB db = getDbHandler(handler);
          db.requestStart();
              try {
                final DBObject p = projection != null ?
                    getDbObjectFromStr(projection) : null;
                final DBObject q = query != null ?
                        getDbObjectFromStr(query) : null;
                final DBCollection coll = db.getCollection(itemToString(col));
                final DBCursor cursor = coll.find(q, p);
                if(options != null) {
                  //final TokenMap map = new FuncParams(Q_MONGODB, null).parse(options);
                  Value keys = options.keys();
                  for(final Item key : keys) {
                    if(!(key instanceof Str))
                        throw new QueryException("String expected, ...");
                    final String k = ((Str) key).toJava();
                    final Value v = options.get(key, null);
                   if(v instanceof Str || v.type().instanceOf(SeqType.ITR)) {
                       if(k.equals("limit")) {
                           System.out.println(v.type());
                           if(v.type().instanceOf(SeqType.ITR_OM)) {
                               long l = ((Item) v).itr(null);
                               cursor.limit((int) l);
                           } else {
                               throw new QueryException("Invalid value...");
                           }
                       }
                   } else if(v instanceof Map) {
                   } else {
                       throw new QueryException("Invalid value 2...");
                   }
                    if(k.equals("limit")) {
                      //cursor.limit(Token.toInt(v));
                    } else if(k.equals("skip")) {
                        //cursor.skip(Token.toInt(v));
                    } else if(k.equals("sort")) {
                        BasicDBObject sort = new BasicDBObject(k, v);
                        sort.append("name", "-1");
                        cursor.sort((DBObject) sort);
                    } else if(k.equals("explain")) {
                        cursor.explain();
                        System.out.println("explain" + v);
                    }
                  }

                }

                return resultToXml(handler, cursor);
              } catch (MongoException e) {
                  throw new QueryException(e.getMessage());
              } finally {
                     db.requestDone();
              }
        }
    /**
     * Mongodb's findOne() function.
     * @param handler
     * @param col
     * @return
     * @throws QueryException
     */
    public Item findOne(final Str handler, final Item col)throws QueryException {
        return findOne(handler, col, null, null);
    }
    /**
     * Mongodb's findOne({'_id':2}) function with query.
     * @param handler
     * @param col
     * @param query
     * @return Item
     * @throws QueryException
     */
    public Item findOne(final Str handler, final Item col, final Item query)
            throws QueryException {
       return findOne(handler, col, query, null);
    }
    /**
     * findOne with query projection and fields real Implementation.
     * @param handler
     * @param col
     * @param query
     * @param field
     * @return Item
     * @throws QueryException
     */
    public Item findOne(final Str handler, final Item col, final Item query,
            final Item projection) throws QueryException {

        final DB db = getDbHandler(handler);
        db.requestStart();
        try {
            final DBObject p = projection != null ?
                    getDbObjectFromStr(projection) : null;
            final DBObject q = query != null ?
                    getDbObjectFromStr(query) : null;
            final DBCollection coll = db.getCollection(itemToString(col));
            final DBObject cursor = coll.findOne(q, p);
            return  objectToXml(handler, cursor);
        } catch (MongoException e) {
            throw new QueryException(e.getMessage());
        } finally {
               db.requestDone();
        }
    }
    /**
     * Insert data.
     * @param handler DB Handler
     * @param col Collection name
     * @param insertString string to insert in json formart.
     * @return
     * @throws Exception 
     */
    public Item insert(final Str handler, final Str col, final Str insertString)
            throws Exception {
        final DB db = getDbHandler(handler);
        db.requestStart();
        try {
            DBObject obj = (DBObject) JSON.parse(insertString.toJava());
            WriteResult wr = db.getCollection(col.toJava()).insert(obj);
           return returnResult(handler, Str.get(wr.toString()));
        } catch (MongoException e) {
           throw new QueryException(db.getLastError().getString("err"));
        } finally {
           db.requestDone();
        }
    }
    /**
     * Mongodb update with two parameters like update({},{}).
     * @param handler
     * @param col
     * @param insertString
     * @return Item
     * @throws Exception 
     */
    public Item update(final Str handler, final Item col, final Item query,
            final Str updatestring) throws Exception {
        final DB db = getDbHandler(handler);
        db.requestStart();
        try {
            DBObject q = (DBObject) JSON.parse(itemToString(query));
            DBObject o = (DBObject) JSON.parse(updatestring.toJava());
            WriteResult wr = db.getCollection(itemToString(col)).update(q, o);
            return returnResult(handler, Str.get(wr.toString()));
        } catch (MongoException e) {
            throw new QueryException(db.getLastError().getString("err"));
        } finally {
           db.requestDone();
        }
    }
    /**
     * Mongodb update with 4 parameters like update({},{}, upsert, multi).
     * @param handler Db Handler string
     * @param col Collection name
     * @param updatestring String to be updated
     * @param upsert true/false for mongodb upsert
     * @param multi true/false for mongodb multi
     * @return Item
     * @throws Exception 
     */
    public Item update(final Str handler, final Item col, final Item query,
            final Str updatestring, final boolean upsert, final boolean multi)
                    throws Exception {
        final DB db = getDbHandler(handler);
        db.requestStart();
        try {
            DBObject q = (DBObject) JSON.parse(itemToString(query));
            DBObject o = (DBObject) JSON.parse(updatestring.toJava());
           WriteResult wr = db.getCollection(itemToString(col)).
                   update(q, o, upsert, multi);
            return returnResult(handler, Str.get(wr.toString()));
        } catch (MongoException e) {
            throw new QueryException(db.getLastError().getString("err"));
        } finally {
           db.requestDone();
        }
    }
    /**
     * Mongodb Save function.
     * @param handler DB handler
     * @param col collection name
     * @param saveStr string to save
     * @return Item
     * @throws Exception 
     */
    public Item save(final Str handler, final Str col, final Item saveStr)
            throws Exception {
        final DB db = getDbHandler(handler);
        db.requestStart();
        try {
           WriteResult wr = db.getCollection(col.toJava()).
                   save(getDbObjectFromStr(saveStr));
           return returnResult(handler, Str.get(wr.toString()));
        } catch (MongoException e) {
            throw new QueryException(db.getLastError().getString("err"));
        } finally {
           db.requestDone();
        }
    }
    /**
     * Mongodb remove().
     * @param handler
     * @param col
     * @param removeStr
     * @throws QueryException
     */
    public void remove(final Str handler, final Item col, final Item remove)
            throws QueryException {
        final DB db = getDbHandler(handler);
        db.requestStart();
        try {
            db.getCollection(itemToString(col)).remove(getDbObjectFromStr(remove));
            DBObject err = db.getLastError();
            if(err != null) {
                throw new QueryException(err.get("err").toString());
            }
        } catch (MongoException e) {
            throw new QueryException(db.getLastError().getString("err"));
        } finally {
           db.requestDone();
        }
    }
    /**
     * Mongodb aggregate().
     * @param handler database handler
     * @param col collection name
     * @param first aggregation compulsary
     * @return Item
     * @throws QueryException
     */
    public Item aggregate(final Str handler, final Item col, final Item first)
            throws QueryException {
        final DB db = getDbHandler(handler);
        db.requestStart();
        try {
           AggregationOutput agg =  db.getCollection(itemToString(col)).aggregate(
                    getDbObjectFromStr(first));
//           return new FNJson(null, Function._JSON_PARSE,
//                   Str.get(JSON.serialize(agg))).item(context, null);
           //Str.get(JSON.serialize(agg));
           for(DBObject dbObj: agg.results()) {
              return objectToXml(handler, dbObj);
            }
        } catch (MongoException e) {
            throw new QueryException(e.getMessage());
        } finally {
           db.requestDone();
        }
        return null;
    }
    /**
     * Mongodb aggregate().
     * @param handler database handler
     * @param col collection name
     * @param first first aggregation compulsary
     * @param more more aggregation options
     * @throws QueryException
     */
    public Item aggregate(final Str handler, final Str col, final Str first,
            final Str more) throws QueryException {
        final DB db = getDbHandler(handler);
        db.requestStart();
        try {
            AggregationOutput agg =  db.getCollection(col.toJava()).aggregate(
                    getDbObjectFromStr(first), getDbObjectFromStr(more));
            for(DBObject dbObj: agg.results()) {
                return objectToXml(handler, dbObj);
              }
        } catch (MongoException e) {
            throw new QueryException(e.getMessage());
        } finally {
           db.requestDone();
        }
        return  null;
    }
    /**
     * count numbers of documents in a collection.
     * @param handler
     * @param col
     * @return number
     * @throws QueryException
     */
    public long count(final Str handler, final Item col) throws QueryException {
        final DB db = getDbHandler(handler);
        long count = db.getCollection(itemToString(col)).count();
        return count;
    }
    /**
     * copy from one collection to another.
     * @param handler
     * @param source
     * @param dest
     * @throws QueryException
     */
    public void copy(final Str handler, final Item source, final Item dest)
            throws QueryException {
        final DB db = getDbHandler(handler);
        db.requestStart();
        try {
            List<DBObject> cursor = db.getCollection(itemToString(source)).
                    find().toArray();
            db.getCollection(itemToString(dest)).insert(cursor);
       } catch (MongoException e) {
           throw new QueryException(db.getLastError().getString("err"));
        } finally {
           db.requestDone();
        }
    }
    /**
     * Drop collection from a database.
     * @param handler
     * @param col
     * @throws QueryException
     */
    public void drop(final Str handler, final Item col)throws QueryException {
        final DB db = getDbHandler(handler);
        db.requestStart();
        try {
            db.getCollection(itemToString(col)).drop();
       } catch (MongoException e) {
           throw new QueryException(db.getLastError().getString("err"));
        } finally {
           db.requestDone();
        }
    }
    /**
     * run database command.
     * @param handler
     * @param command
     * @throws Exception 
     */
    public Item runCommand(final Str handler, final Str command)throws Exception {
        final DB db = getDbHandler(handler);
        db.requestStart();
        try {
           CommandResult result = db.command(command.toJava());
           return returnResult(handler, Str.get(result.toString()));
       } catch (MongoException e) {
           throw new QueryException(db.getLastError().getString("err"));
        } finally {
           db.requestDone();
        }
    }
    /**
     * convert json to xml.
     * @param json
     * @return Items in xml  format
     * @throws QueryException
     */
    public Item jsonToXml(final Str json) throws QueryException {
        return new FNJson(staticContext, null, Function._JSON_PARSE, json).
                item(queryContext, null);
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
    }
}