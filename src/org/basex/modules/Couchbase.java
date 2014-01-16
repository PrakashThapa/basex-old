package org.basex.modules;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.internal.OperationFuture;

import org.basex.query.QueryContext;
import org.basex.query.QueryException;
import org.basex.query.func.FNFt;
import org.basex.query.func.FuncOptions;
import org.basex.query.iter.Iter;
import org.basex.query.value.Value;
import org.basex.query.value.item.Item;
import org.basex.query.value.item.QNm;
import org.basex.query.value.item.Str;
import org.basex.query.value.map.Map;
import org.basex.query.value.type.SeqType;
import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewDesign;
import com.couchbase.client.protocol.views.ViewResponse;
import com.couchbase.client.protocol.views.ViewRow;
import com.couchbase.client.vbucket.ConfigurationException;


/**
 * CouchBase extension of Basex.
 *
 * @author BaseX Team 2005-13, BSD License
 * @author Prakash Thapa
 */
public class Couchbase extends Nosql {
    /** URL of this module. */
    private static final String COUCHBASE_URL = "http://basex.org/modules/couchbase";
    /** QName of Couchbase options. */
    private static final QNm Q_COUCHBASE = QNm.get("couchbase", "options",
            COUCHBASE_URL);
    /** Couchbase instances. */
    private HashMap<String, CouchbaseClient> couchbaseclients =
            new HashMap<String, CouchbaseClient>();
    /** Couchbase options. */
    private HashMap<String, NosqlOptions> couchopts = new HashMap
            <String, NosqlOptions>();
    /** nodes array. */
    //private ArrayList<URI> nodes = new ArrayList<URI>();
    public Couchbase() {
        super(Q_COUCHBASE);
    }
    public Iter search(final QueryContext ctx) throws QueryException {
        final NosqlOptions opts = new NosqlOptions();
        new FNFt(null, null, null).checkOptions(2, Q_COUCHBASE, opts, ctx);
        return null;
    }
    /**
     * Couchbase connection with url host bucket.
     * @param url
     * @param bucket
     * @param password
     * @return Connection handler of Couchbase url
     * @throws Exception
     */
    public Str connection(final Str url, final Str bucket, final Str password)
            throws Exception {
        return connection(url, bucket, password, null);
    }
    /**
     * Couchbase connection with url host bucket and with option.
     * @param url
     * @param bucket
     * @param password
     * @return Connection handler of Couchbase url
     * @throws Exception
     */
    public Str connection(final Str url, final Str bucket, final Str password,
            final Map options) throws Exception {
        final NosqlOptions opts = new NosqlOptions();
        if(options != null) {
            new FuncOptions(Q_COUCHBASE, null).parse(options, opts);
        }
        try {
            String handler = "cbClient" + couchbaseclients.size();
            List<URI> hosts = Arrays.asList(new URI(url.toJava()));
//            CouchbaseClient client = new CouchbaseClient(hosts,
//                    bucket.toJava(), password.toJava());
            CouchbaseConnectionFactory cf = new CouchbaseConnectionFactory(hosts,
                    bucket.toJava(), password.toJava());
            CouchbaseClient client    =   new CouchbaseClient(cf);
            if(options != null) {
                couchopts.put(handler, opts);
            }
            couchbaseclients.put(handler, client);
            return Str.get(handler);
        } catch (ConfigurationException e) {
            throw new QueryException("Invalid Authentication parameters");
        }catch (Exception ex) {
              throw new QueryException(ex);
          }
    }
    /**
     * get CouchbaseClinet from the hashmap.
     * @param handler
     * @return connection instance.
     * @throws QueryException
     */
    private CouchbaseClient getClient(final Str handler) throws QueryException {
        String ch = handler.toJava();
        try {
            final CouchbaseClient client = couchbaseclients.get(ch);
            if(client == null)
                throw new QueryException("Unknown CouchbaseClient handler: '" + ch + "'");
            return client;
        } catch (final Exception ex) {
            throw new QueryException(ex);
        }
    }
    /**
     * get Couchbase option from particular db handler.
     * @param handler
     * @return MongoOptions
     */
    private NosqlOptions getCouchbaseOption(final Str handler) {
        NosqlOptions opt = couchopts.get(handler.toJava());
        if(opt != null)
            return opt;
        else
            return null;
    }
    /**
     * This will check the assigned options and then return the final result
     * process by parent class.
     * @param handler
     * @param json
     * @return
     * @throws Exception
     */
    private Item returnResult(final Str handler, final Str json)
            throws Exception {
        NosqlOptions opt =   getCouchbaseOption(handler);
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
     * add new document.
     * @param handler Database handler
     * @param key
     * @param value
     * @throws QueryException
     */
    public Item add(final Str handler, final Item key, final Item doc)
            throws QueryException {
        return put(handler, key, doc, "add");
    }
    /**
     * Set method of Couchbase.
     * @param handler
     * @param key
     * @param doc
     * @return
     * @throws QueryException
     */
    public Item set(final Str handler, final Item key,  final Item doc)
            throws QueryException {
        return put(handler, key, doc, "set");
    }
    /**
     * Replace method of Couchbase document with condition.
     * @param handler
     * @param key
     * @param doc
     * @return Item
     * @throws QueryException
     */
    public Item replace(final Str handler, final Item key, final Item doc)
            throws QueryException {
       return put(handler, key, doc, "replace");
    }
    /**
     * Append document with key.
     * @param handler
     * @param key
     * @param doc
     * @return Item
     * @throws QueryException
     */
    public Item append(final Str handler, final Item key, final Item doc)
            throws QueryException {
        //CouchbaseClient client = getClient(handler);
        return put(handler, key, doc, null);
        /*
        OperationFuture<Boolean> result = null;
        Str existing = (Str) get(handler, key);
        final StringBuilder s = new StringBuilder();
        s.append('[');
        s.append(existing.toJava());
        s.append(',');
        s.append(itemToString(doc));
        s.append(']');
        System.out.println(s.toString());
        try {
            result = client.replace(
                    itemToString(key), s);
            String msg = result.getStatus().getMessage();
            if(result.get().booleanValue()) {
                return Str.get(msg);
            } else {
                throw new QueryException("operation fail " + msg);
            }
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
        */
    }
    /**
     * document addition.
     * @param handler
     * @param key
     * @param doc
     * @param options {set,put,append,replace}
     * @return Item
     * @throws QueryException
     */
    public Item put(final Str handler, final Item key, final Item doc,
            final String type) throws QueryException {
        CouchbaseClient client = getClient(handler);
        OperationFuture<Boolean> result = null;
        try {
            if(type != null) {
                if(type.equals("add")) {
                 result = client.add(
                           itemToString(key), itemToJsonString(doc));
                } else if(type.equals("replace")) {
                    result = client.replace(
                           itemToString(key), itemToJsonString(doc));
                } else if(type.equals("set")) {
                   result = client.set(
                           itemToString(key), itemToJsonString(doc));
                } else {
                   result = client.append(
                           itemToString(key), itemToString(doc));
                }
            } else {
                result = client.append(
                        itemToString(key), itemToString(doc));
                //return append(handler, key, doc);
            }
            String msg = result.getStatus().getMessage();
            if(result.get().booleanValue()) {
                return Str.get(msg);
            } else {
                throw new QueryException("operation fail " + msg);
            }
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
    }
    /**
     * get document with key.
     * @param handler
     * @param key
     * @return Item
     * @throws QueryException
     */
    public Item get(final Str handler, final Item key) throws QueryException {
        CouchbaseClient client = getClient(handler);
        try {
            Object result =  client.get(itemToString(key));
            if(result != null) {
                 Str json = Str.get((String) result);
                 return returnResult(handler, json);
            } else
              throw new QueryException("Element is empty");
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
    }
    /**
     * Get document with options.
     * @param handler
     * @param doc
     * @param options
     * @return
     * @throws QueryException
     */
    public Str get(final Str handler, final Str doc, final Map options)
         throws QueryException {
        CouchbaseClient client = getClient(handler);
        try {
            if(options != null) {
                Value keys = options.keys();
                for(final Item key : keys) {
                    if(!(key instanceof Str))
                        throw new QueryException("String expected, ...");
                    final String k = ((Str) key).toJava();
                    final Value v = options.get(key, null);
                    if(k.equals("add")) {
                        if(v.type().instanceOf(SeqType.STR)) {
                            Str s = (Str) v.toJava();
                            return s;
                        }
                    }
                }
            }
            final Object o = client.get(doc.toJava());
            if(o != null) {
                return Str.get(o.toString());
            } else
                throw new QueryException("Element is empty");
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
    }
    public Item getbulk(final Str handler, final Item options) throws QueryException {
         try {
             if(options.size() < 1) {
                 throw new QueryException("key set is empty");
             }
             List<String> keys = new ArrayList<String>();
             for (Value v: options) {
                String s = (String) v.toJava();
                keys.add(s);
             }
//             java.util.Map<String, Object> s = client.getBulk(keys);
//             Object s1 = client.getBulk(keys);
//             return new FNJson(staticContext, null, Function._JSON_PARSE, Str.get(s2)).
//                     item(queryContext, null);
         } catch (Exception ex) {
            throw new QueryException(ex);
        }
        return handler;
    }
    /**
     * remove document by key.
     * @param handler
     * @param key
     * @return
     * @throws QueryException
     */
    public Item remove(final Str handler, final Str key) throws QueryException {
        return delete(handler, key);
    }
    /**
     * Delete document by document key.
     * @param handler
     * @param key
     * @return
     * @throws QueryException
     */
    public Item delete(final Str handler, final Str key) throws QueryException {
        CouchbaseClient client = getClient(handler);
        try {
            OperationFuture<Boolean> result = client.delete(key.toJava());
            String msg = result.getStatus().getMessage();
            if(result.get().booleanValue()) {
                return Str.get(msg);
            } else {
                throw new QueryException("operation fail:" + msg);
            }
        } catch (Exception ex) {
            throw new QueryException(ex);
        }
    }
    /**
     * Create view without reduce method.
     * @param handler
     * @param doc
     * @param viewName
     * @param map
     * @return
     * @throws QueryException
     */
    public Item createView(final Str handler, final Str doc, final Str viewName,
            final Str map) throws QueryException {
        return createView(handler, doc, viewName, map, null);
    }
    /**
     * Create view with reduce method.
     * @param handler Database handler
     * @param doc
     * @param viewName
     * @param map
     * @param reduce
     * @return
     * @throws QueryException
     */
    public Item createView(final Str handler, final Str doc, final Str viewName,
            final Str map, final Str reduce) throws QueryException {
        CouchbaseClient client = getClient(handler);
        if(map == null) {
            throw new QueryException("map function is empty");
        }
        try {
            DesignDocument designDoc = new DesignDocument(doc.toJava());
            ViewDesign viewDesign;
            if(reduce != null) {
               viewDesign = new ViewDesign(viewName.toJava(),
                       map.toJava(), reduce.toJava());
            } else {
                viewDesign = new ViewDesign(viewName.toJava(), map.toJava());
            }
            designDoc.getViews().add(viewDesign);
           Boolean success = client.createDesignDoc(designDoc);
           if(success) {
               return Str.get("ok");
           } else {
               throw new QueryException("There is something wrong");
           }
        } catch (Exception e) {
            throw new QueryException(e);
        }
    }
    /**
     * Get data from view without any option.
     * @param handler
     * @param doc
     * @param viewName
     * @return item
     * @throws QueryException
     */
    public Item getview(final Str handler, final Str doc, final Str viewName)
            throws QueryException {
        return getview(handler, doc, viewName, null);
    }
    /**
     * view with mode Option.
     * @param handler
     * @param doc
     * @param viewName
     * @param mode
     * @param options options like limit and so on(not completed)
     * @return
     * @throws QueryException
     */
    public Item getview(final Str handler, final Str doc, final Str viewName,
            final Map options) throws QueryException {
        final CouchbaseClient client = getClient(handler);
        Query q = new Query();
        q.setIncludeDocs(true);
        boolean valueOnly = false;
        if(options != null) {
            Value keys = options.keys();
            for(final Item key : keys) {
                if(!(key instanceof Str))
                    throw new QueryException("String expected, ...");
                final String k = ((Str) key).toJava();
                final Value v = options.get(key, null);
                if(k.equals("viewmode")) {
                    System.setProperty("viewmode", v.toJava().toString());
                } else if(k.equals("limit")) {
                    if(v.type().instanceOf(SeqType.ITR_OM)) {
                        long l = ((Item) v).itr(null);
                        q.setLimit((int) l);
                    } else {
                        throw new QueryException("Invalid value number expected...");
                    }
                } else if(k.equals("stale")) {
                    String s = ((Item) v).toString();
                    if(s.equals("ok"))
                        q.setStale(Stale.OK);
                    else if(s.equals("false"))
                        q.setStale(Stale.FALSE);
                    else if(s.equals("update_after"))
                        q.setStale(Stale.UPDATE_AFTER);
                } else if(k.equals("key")) {
                    String s = ((Item) v).toString();
                    q.setKey(s);
                } else if(k.equals("startkey")) {
                    String s = ((Item) v).toString();
                    q.setStartkeyDocID(s);
                } else if(k.equals("endkey")) {
                    String s = ((Item) v).toString();
                    q.setEndkeyDocID(s);
                } else if(k.equals("skip")) {
                    if(v.type().instanceOf(SeqType.ITR_OM)) {
                        long l = ((Item) v).itr(null);
                        q.setSkip((int) l);
                    } else {
                        throw new QueryException("Invalid value number expected...");
                    }
                } else if(k.equals("range")) {
                    if(v.iter().size() == 2) {
                        q.setRange(v.iter().get(0).toString(), v.iter().
                                get(1).toString());
                    }
                } else if(k.equals("descending")) {
                    boolean desc = ((Item) v).bool(null);
                    q.setDescending(desc);
                } else if(k.equals("debug")) {
                    boolean d = ((Item) v).bool(null);
                    q.setDebug(d);
                } else if(k.toLowerCase().equals("valueonly")) {
                    valueOnly = ((Item) v).bool(null);
                }
            }
        }
        q.setIncludeDocs(true);
        try {
            View view = client.getView(doc.toJava(), viewName.toJava());
            ViewResponse response = client.query(view, q);
            Str json = valueOnly ? viewResponseToJsonValueOnly(response)
                    : viewResponseToJson(response);
            return returnResult(handler, json);
        } catch (Exception e) {
            throw new QueryException(e);
        }
    }
    /**
     * create Json format string from view Response.
     * @param viewResponse
     * @return
     */
    private Str viewResponseToJson(final ViewResponse viewResponse) {
        final StringBuilder json = new StringBuilder();
        json.append("{ ");
        for (ViewRow v: viewResponse) {
            if(json.length() > 2) json.append(", ");
            json.append('"').append(v.getKey()).append('"').append(" : ");
            String value = v.getValue();
            if(value != null) {
                value = value.trim();
                if(value.charAt(0) == '{' || value.charAt(0) == '[') {
                    json.append(v.getValue());
                } else {
                    json.append('"').append(value.replaceAll("\"", "\\\"")).append('"');;
                }
            } else {
                json.append('"').append("").append('"');
            }
        }
        json.append(" } ");
        return Str.get(json.toString());
    }
    /**
     * create Json format string from view Response.
     * @param viewResponse
     * @return
     */
    private Str viewResponseToJsonValueOnly(final ViewResponse viewResponse) {
        final StringBuilder json = new StringBuilder();
        if(viewResponse.size() > 1) {
            json.append("[ ");
        }
        for (ViewRow v: viewResponse) {
            if(json.length() > 2) json.append(", ");
            //json.append('"').append(v.getKey()).append('"').append(" : ");
            String value = v.getValue();
            if(value != null) {
                value = value.trim();
                if(value.charAt(0) == '{' || value.charAt(0) == '[') {
                    json.append(v.getValue());
                } else {
                    json.append('"').append(value.replaceAll("\"", "\\\"")).append('"');;
                }
            } else {
                json.append('"').append("").append('"');
            }
        }
        if(viewResponse.size() > 1) {
            json.append(" ] ");
        }
        return Str.get(json.toString());
    }
    /**
     *  close database instanses.
     * @param handler
     * @throws QueryException
     */
    public void shutdown(final Str handler) throws QueryException {
        shutdown(handler, null);
    }
    /**
     *  close database connection after certain time.
     * @param handler
     * @param time in seconds
     * @throws QueryException
     */
    public void shutdown(final Str handler, final Item time)
            throws QueryException {
        CouchbaseClient client = getClient(handler);
        if(time != null) {
            if(!time.type().instanceOf(SeqType.ITR)) {
                throw new QueryException("Time is not valid, ...");
            }
            long seconds = ((Item) time).itr(null);
            boolean result = client.shutdown(seconds, TimeUnit.SECONDS);
            if(!result) {
                throw new QueryException("cannot be shutdown now");
            }
        } else {
            client.shutdown();
        }
    }
}