package org.basex.modules;

import java.util.ArrayList;
import java.util.HashMap;

import org.basex.query.QueryException;
import org.basex.query.func.FuncOptions;
import org.basex.query.value.Value;
import org.basex.query.value.item.Item;
import org.basex.query.value.item.QNm;
import org.basex.query.value.item.Str;
import org.basex.query.value.map.Map;
import org.basex.query.value.seq.StrSeq;
import org.basex.query.value.type.SeqType;
import com.dkhenry.RethinkDB.RqlConnection;
import com.dkhenry.RethinkDB.RqlCursor;
import com.dkhenry.RethinkDB.RqlMethodQuery.Delete;
import com.dkhenry.RethinkDB.RqlMethodQuery.Update;
import com.dkhenry.RethinkDB.RqlObject;
import com.dkhenry.RethinkDB.RqlQuery;
import com.dkhenry.RethinkDB.RqlQuery.Table;
import com.dkhenry.RethinkDB.errors.RqlDriverException;
import com.mongodb.util.JSON;

/**
 * This is the primary class for RethinkDB processing in Basex.
 *
 * @author BaseX Team 2005-14, BSD License
 * @author Prakash Thapa
 */
public class RethinkDB extends Nosql {
    protected static final String ID = "id";
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
    /** Rethinkdb instances. */
    private HashMap<String, RqlConnection> rethinkClient =
            new HashMap<String, RqlConnection>();
    protected HashMap<String, NosqlOptions> rethinkopts = new HashMap
            <String, NosqlOptions>();
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
            rethinkopts.put(handler, opts);
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
     * get rethinkdb option from particular db handler.
     * @param handler
     * @return MongoOptions
     * @throws QueryException
     */
    protected NosqlOptions getRethinkDBOption(final Str handler) throws QueryException {
        NosqlOptions opt = rethinkopts.get(handler.toJava());
        if(opt == null)
            throw new QueryException("invalid RethinkDB Options");
        return opt;
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
        com.dkhenry.RethinkDB.RqlTopLevelQuery.DB db = r.db(
                getRethinkDBOption(handler).get(NosqlOptions.DATABASE));
        return db;

    }
    private Item run(final Str handler, final RqlQuery q)
            throws QueryException {
        RqlConnection r = getRethikClient(handler);
        try {
           RqlCursor result = r.run(q);
           System.out.println(result);
           return processRqlCursor(handler, result);
        } catch (RqlDriverException e) {
           throw new QueryException(e);
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
    }
    private Table getTable(final Str handler, final Str table) throws QueryException {
        com.dkhenry.RethinkDB.RqlTopLevelQuery.DB db = getRethikClientDB(handler);
        return  db.table(table.toJava());
    }
    private Item processRqlCursor(final Str handler, final RqlCursor cursor)
            throws Exception {
        ArrayList<Object> array = new ArrayList<Object>();
        for(RqlObject o : cursor) {
            try {
                java.util.Map<String, Object> map = o.getMap();
                array.add(map);
            } catch (RqlDriverException e) {
                throw new QueryException(e.getMessage());
            }
        }
//       Str json = (array.size() == 1) ? Str.get(JSON.serialize(array.get(0)))
//               : Str.get(JSON.serialize(array));
        Str json = Str.get(JSON.serialize(array));
       return returnResult(handler, json);
    }
    /**
     * This will check the assigned options and then return the final result
     * process by parent class.
     * @param handler
     * @param json
     * @return
     * @throws Exception
     */
    protected Item returnResult(final Str handler, final Str json)
            throws Exception {
        Str j = json;
        if(j == null) {
            j =  Str.get("{}");
        }
        NosqlOptions opts = getRethinkDBOption(handler);
        if(opts != null) {
            return finalResult(j, opts);
        } else {
            return finalResult(j, null);
        }
    }
    /**
     * create new database.
     * @param handler
     * @param db
     * @throws QueryException
     */
    public Item dbCreate(final Str handler, final Str db) throws QueryException {
        RqlConnection r = getRethikClient(handler);
        try {
          return run(handler, r.db_create(db.toJava()));
        } catch (Exception e) {
            throw new QueryException(e.getMessage());
        }
    }
    /**
     * drop database.
     * @param handler
     * @param db
     * @throws QueryException
     */
    public Item dbDrop(final Str handler, final Str db) throws QueryException {
        RqlConnection r = getRethikClient(handler);
        try {
           return run(handler, r.db_drop(db.toJava()));
        } catch (Exception e) {
            throw new QueryException(e.getMessage());
        }
    }
    public Item dbList(final Str handler, final Str db) throws QueryException {
        RqlConnection r = getRethikClient(handler);
        try {
          return run(handler, r.db_list());
        } catch (Exception e) {
            throw new QueryException(e.getMessage());
        }
    }
    public Item tableCreate(final Str handler, final Str table)
            throws QueryException {
        com.dkhenry.RethinkDB.RqlTopLevelQuery.DB db = getRethikClientDB(handler);
        return run(handler, db.table_create(table.toJava()));
    }
    public Item tableList(final Str handler, final Str table, final Str indexname)
            throws QueryException {
        com.dkhenry.RethinkDB.RqlTopLevelQuery.DB db = getRethikClientDB(handler);
        try {
            return run(handler, db.table_list());
        } catch (Exception e) {
            throw new QueryException(e.getMessage());
        }
    }
    public Item dropTable(final Str handler, final Str table) throws QueryException {
        com.dkhenry.RethinkDB.RqlTopLevelQuery.DB db = getRethikClientDB(handler);
        try {
            return run(handler, db.table_drop(table.toJava()));
        } catch (Exception e) {
            throw new QueryException(e.getMessage());
        }
    }
    public Item indexCreate(final Str handler, final Str table, final Str indexname)
            throws QueryException {
        com.dkhenry.RethinkDB.RqlTopLevelQuery.DB db = getRethikClientDB(handler);
        try {
            return run(handler, db.table(table.toJava()).
                    index_create(indexname.toJava()));
        } catch (Exception e) {
            throw new QueryException(e.getMessage());
        }
    }
    public Item indexList(final Str handler, final Str table)
            throws QueryException {
        com.dkhenry.RethinkDB.RqlTopLevelQuery.DB db = getRethikClientDB(handler);
        try {
            return run(handler, db.table(table.toJava()).index_list());
        } catch (Exception e) {
            throw new QueryException(e.getMessage());
        }
    }
    public Item indexDrop(final Str handler, final Str table, final Str indexname)
            throws QueryException {
        com.dkhenry.RethinkDB.RqlTopLevelQuery.DB db = getRethikClientDB(handler);
        try {
            return run(handler, db.table(table.toJava()).
                    index_drop(indexname.toJava()));
        } catch (Exception e) {
            throw new QueryException(e.getMessage());
        }
    }
    public Item insert(final Str handler, final Str table, final Item insertString)
            throws QueryException {
        return insert(handler, table, insertString, null);
    }
    public Item insert(final Str handler, final Str table, final Item insertString,
            final Map options) throws QueryException {
        com.dkhenry.RethinkDB.RqlTopLevelQuery.DB db = getRethikClientDB(handler);
        if(insertString == null) {
            throw new QueryException("insert string cannot be empty");
        }
        Object json;
        if(insertString instanceof Str) {
             json = JSON.parse((String) insertString.toJava());
        } else {
            json = ((Map) insertString).toJava();
        }
        try {
            RqlQuery i;
            if(options == null) {
                i = db.table(table.toJava()).insert(json);
            } else {
                i = db.table(table.toJava()).insert(json,
                        options.toJava());
            }
           return run(handler, i);
        } catch (Exception e) {
            throw new QueryException(e.getMessage());
        }
    }
    public Item update(final Str handler, final Str table, final Map json)
            throws QueryException {
        return update(handler, table, json, null);
    }
    public Item update(final Str handler, final Str table, final Map json,
            final Map filter) throws QueryException {
        com.dkhenry.RethinkDB.RqlTopLevelQuery.DB db = getRethikClientDB(handler);
        if(json == null) {
            throw new QueryException("insert string cannot be empty");
        }
        Update u;
        if(filter == null) {
            u = db.table(table.toJava()).update(json.toJava());
        } else {
            u = db.table(table.toJava()).filter(filter.toJava()).update(json.toJava());
            Value keys = filter.keys();
            for(final Item key : keys) {
                if(!(key instanceof Str)) {
                    throw new QueryException("Key " + key.toJava() + " should be string");
                }
                final Value value = filter.get(key, null);
                final String v = (String) value.toJava();
                final String k = (String) key.toJava();
                if(value instanceof Str) {
                    if(k.toLowerCase().equals(ID)) {
                        db.table(table.toJava()).get(v).update(json.toJava());
                        break;
                    }
                }
            }
        }
        return run(handler, u);
    }
    public Item table(final Str handler, final Str table)
            throws QueryException {
        return run(handler, getTable(handler, table));
    }
    public Item get(final Str handler, final Str table, final Item id)
            throws QueryException {
        if(id == null) {
            throw new QueryException("id should not be empty");
        }
        if(id.type().instanceOf(SeqType.ITR_OM) ||  id.type().mayBeNumber()) {
            long l = ((Item) id).itr(null);
            return run(handler, getTable(handler, table).get((int) l));
        } else {
            String s = ((Str) id).toJava();
            return run(handler, getTable(handler, table).
                    get(s));
        }
    }
    public Item getAll(final Str handler, final Str table, final Value ids)
            throws QueryException {
        Object s;
        if(ids instanceof Str) {
            s = ((Str) ids).toJava();
       } else if((Value) ids instanceof StrSeq) {
             s = ((StrSeq) ids).toJava();
        } else  {
            s = ids.toJava();
        }
        return run(handler, getTable(handler, table).get_all(s));
    }
    public Item hasFields(final Str handler, final Str table, final Item fields)
            throws QueryException {
        if(fields instanceof Map) {
            return run(handler, getTable(handler, table).
                    has_fields(((Map) fields).toJava()));
        }
        String s = (String) fields.toJava();
        return run(handler, getTable(handler, table).has_fields(s));
    }
    public Item sync(final Str handler, final Str table)
            throws QueryException {
       return run(handler, getTable(handler, table));
    }
    public Item delete(final Str handler, final Str table)
            throws QueryException {
       return  delete(handler, table, null);
    }
    public Item delete(final Str handler, final Str table, final Item filter)
            throws QueryException {
        return delete(handler, table, filter, null);
    }
    public Item delete(final Str handler, final Str table, final Item filter,
            final Map options) throws QueryException {
        Delete t;
        final HashMap<Object, Object> opt = (options == null) ? null :
            ((Map) options).toJava();
        if(filter != null && !filter.isEmpty()) {
            if(filter instanceof Map) {
                t = (opt == null) ? getTable(handler, table).
                        filter(((Map) filter).toJava()).delete()
                        : getTable(handler, table).filter(((Map) filter).toJava()).
                        delete(opt);
            } else {
                // if(filter instanceof Str || filter.type().instanceOf(SeqType.ITR_OM))
                //t = getTable(handler, table).get(filter.toJava()).delete();
                t = (opt == null) ? getTable(handler, table).get(filter.toJava()).delete()
                        : getTable(handler, table).get(filter.toJava()).
                        delete(opt);
            }
        } else {
            t = (opt == null) ? getTable(handler, table).delete()
                    : getTable(handler, table).delete(opt);
        }
       return run(handler, t);
    }
    public void close(final Str handler) throws QueryException {
        try {
            getRethikClient(handler).close();
        } catch (RqlDriverException e) {
            throw new QueryException(e.getMessage());
        } catch (QueryException e) {
            throw new QueryException(e.getMessage());
        }
    }
}