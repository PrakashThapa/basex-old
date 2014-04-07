package org.basex.modules;

import java.util.HashMap;
import org.basex.query.QueryException;
import org.basex.query.func.FuncOptions;
import org.basex.query.value.item.QNm;
import org.basex.query.value.item.Str;
import org.basex.query.value.map.Map;
import com.dkhenry.RethinkDB.RqlConnection;
import com.dkhenry.RethinkDB.RqlCursor;
import com.dkhenry.RethinkDB.RqlQuery;
import com.dkhenry.RethinkDB.errors.RqlDriverException;

/**
 * This is the primary class for RethinkDB processing in Basex.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Prakash Thapa
 */
public class RethinkDB extends Nosql {
    protected static final String LIMIT = "limit";
    protected static final String SORT = "SORT";
    protected static final String SKIP = "skip";
    protected static final String COUNT = "count";
    protected static final String EXPLAIN = "explain";
    protected static final String MAP = "map";
    protected static final String REDUCE = "reduce";
    protected static final String QUERY = "query";
    protected static final String FINALIZE = "finalalize";
    /** URL of MongDB module. */
    private static final String RETHINKDB_URL = "http://basex.org/modules/rethinkdb";
    /** QName of MongoDB options. */
    private static final QNm Q_RETHINKDB = QNm.get("rethinkdb", "options",
            RETHINKDB_URL);
    /** mongoclient instances. */
    private HashMap<String, RqlConnection> rethinkClient =
            new HashMap<String, RqlConnection>();
    private HashMap<String, com.dkhenry.RethinkDB.RqlTopLevelQuery.DB> dbs =
            new HashMap<String, com.dkhenry.RethinkDB.RqlTopLevelQuery.DB>();
    public RethinkDB() {
        super(Q_RETHINKDB);
    }
    /**
     * Connect parameters in map like: {"host":"localhost","port":27017,
     * "database":"test", "username":"user", "password":"pass"}.
     * @param connectionMap
     * @return Str Key of HashMap that contains all DB Object of Mongodb
     * connection instances.
     * @throws QueryException
     */
    public Str connection(final Map connectionMap) throws QueryException {
        final NosqlOptions opts = new NosqlOptions();
        if(connectionMap != null) {
            new FuncOptions(Q_RETHINKDB, null).parse(connectionMap, opts);
        }
        String handler = "Client" + rethinkClient.size();
        try {
            RqlConnection r = RqlConnection.connect((String) opts.get(
                  NosqlOptions.HOST), (int) opts.get(NosqlOptions.PORT));
            rethinkClient.put(handler, r);
            return Str.get(handler);
        } catch (RqlDriverException e) {
            throw new QueryException(e);
        }
    }
    /**
     * get rethinkclient r from hashmap.
     * @param handler Str
     * @return RqlConnection
     * @throws QueryException
     */
    private RqlConnection getRethikClient(final Str handler)
            throws QueryException {
        RqlConnection r = rethinkClient.get(handler.toJava());
        if(r == null) {
            throw new QueryException("invalid client");
        }
        return r;

    }
    /**
     * get rethinkclient r from hashmap.
     * @param handler Str
     * @return RqlConnection
     * @throws QueryException
     */
    private com.dkhenry.RethinkDB.RqlTopLevelQuery.DB getRethikClientDB(final Str handler)
            throws QueryException {
        RqlConnection r = rethinkClient.get(handler.toJava());
        com.dkhenry.RethinkDB.RqlTopLevelQuery.DB db = r.db(NosqlOptions.DATABASE);
        return db;

    }
    public RqlCursor run(final Str handler, final RqlQuery q)
            throws QueryException {
        RqlConnection r = getRethikClient(handler);
        try {
           RqlCursor result = r.run(q);
           System.out.println(result.toString());
           return result;
        } catch (RqlDriverException e) {
           throw new QueryException(e);
        }
    }
    /**
     * 
     * @param handler
     * @param db
     * @throws QueryException
     */
    public void createDb(final Str handler, final Str db) throws QueryException {
        RqlConnection r = getRethikClient(handler);
        try {
           run(handler, r.db_create(db.toJava()));
        } catch (Exception e) {
            throw new QueryException(e.getMessage());
        }
    }
    public void createTable(final Str handler, final Str table) throws QueryException {
        com.dkhenry.RethinkDB.RqlTopLevelQuery.DB db = getRethikClientDB(handler);
        System.out.println(db.toString());
        db.table_create(table.toJava());
    }
    public void close(final Str handler){
        
    }
}