package org.basex.modules;

import java.util.HashMap;
import java.util.List;

import org.basex.query.QueryException;
import org.basex.query.func.FuncOptions;
import org.basex.query.value.item.Int;
import org.basex.query.value.item.QNm;
import org.basex.query.value.item.Str;
import org.basex.query.value.map.Map;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

/**
 * CouchDB extension of Basex.
 *
 * @author BaseX Team 2005-13, BSD License
 * @author Prakash Thapa
 */
public class CouchDB extends Nosql {
    public CouchDB() {
        super(Q_COUCHDB);
    }
    /** URL of this module. */
    private static final String COUCHDB_URL = "http://basex.org/modules/couchdb";
    /** QName of CouchDB options. */
    private static final QNm Q_COUCHDB = QNm.get("couchdb", "options",
            COUCHDB_URL);
    /** Couchdb Clients instances. */
    private HashMap<String, HttpClient> couchdbClients =
            new HashMap<String, HttpClient>();
    /** DB instances instances. */
    private HashMap<String, CouchDbConnector> dbs = new HashMap<String,
            CouchDbConnector>();
    /** Mongo Options instances. */
    private HashMap<String, NosqlOptions> couchdbopts = new HashMap<String,
            NosqlOptions>();
    /**
     * Couchdb Connection from URLstructure: mongodb://root:root@localhost/test.
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
            new FuncOptions(Q_COUCHDB, null).parse(options, opts);
        }
//        HttpClient authenticatedHttpClient = new StdHttpClient.Builder().
//                url("http://localhost:5984").username("admin").
//                password("secret").build();
        String handler = "couchdbclient" + couchdbClients.size();
        try {
            HttpClient couchdbClient = new StdHttpClient.Builder().build();
            couchdbClients.put(handler, couchdbClient);
            final String dbh = "DB" + dbs.size();
            try {
                CouchDbInstance dbInstance = new StdCouchDbInstance(couchdbClient);
                CouchDbConnector db = dbInstance.createConnector("anna",
                        true);
                dbs.put(dbh, db);
                if(options != null) {
                    couchdbopts.put(dbh, opts);
                }
                return Str.get(handler);
                } catch (final Exception ex) {
                    throw new QueryException(ex);
                 }
            } catch (final Exception ex) {
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
//      HttpClient authenticatedHttpClient = new StdHttpClient.Builder().
//      url("http://localhost:5984").username("admin").
//      password("secret").build();
        return null;
        }
    /**
     * database instance select from httpclient instance.
     * @param handler couchdb httpclient hashmap
     * @param dbName Databasename to connect
     * @return DB handler of hashmap
     * @throws QueryException
     */
    public Str selectDb(final Str handler, final Str dbName)
            throws QueryException {
        String key = handler.toJava();
        final HttpClient client = couchdbClients.get(key);
        if(client == null)
            throw new QueryException("Unknown Couchdb handler: '" + key + "'");
        final String dbh = "DB" + dbs.size();
        try {
            CouchDbInstance dbInstance = new StdCouchDbInstance(client);
            CouchDbConnector db = dbInstance.createConnector(dbName.toJava(), true);
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
    private CouchDbConnector getDbHandler(final Str handler) throws QueryException {
          CouchDbConnector db = dbs.get(handler.toJava());
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
    @SuppressWarnings("unused")
    private NosqlOptions getMongoOption(final Str handler) {
        NosqlOptions opt = couchdbopts.get(handler.toJava());
        if(opt != null)
            return opt;
        else
            return null;
    }
    public List<String> find(final Str handler, final Str dbname) throws QueryException {
          CouchDbConnector db = getDbHandler(selectDb(handler, dbname));
         List<String> x = db.getAllDocIds();
         return x;
    }
}