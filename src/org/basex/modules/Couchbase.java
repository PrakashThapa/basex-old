package org.basex.modules;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.internal.OperationFuture;

import org.basex.query.QueryException;
import org.basex.query.QueryModule;
import org.basex.query.func.FNJson;
import org.basex.query.func.Function;
import org.basex.query.value.Value;
import org.basex.query.value.item.Item;
import org.basex.query.value.item.Str;
import org.basex.query.value.map.Map;
import org.basex.query.value.type.SeqType;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.protocol.views.DesignDocument;
import com.couchbase.client.protocol.views.Query;
import com.couchbase.client.protocol.views.Stale;
import com.couchbase.client.protocol.views.View;
import com.couchbase.client.protocol.views.ViewDesign;
import com.couchbase.client.protocol.views.ViewResponse;


/**
 * CouchBase extension of Basex.
 *
 * @author BaseX Team 2005-13, BSD License
 * @author Prakash Thapa
 */
public class Couchbase extends QueryModule {
    /** URL of this module. */
    //private static final String COUCHBASE_URL = "http://basex.org/modules/couchbase";
    /** QName of Couchbase options. */
//    private static final QNm Q_COUCHBASE = QNm.get("couchbase", "options",
//            COUCHBASE_URL);
    /** Couchbase instances. */
    private HashMap<String, CouchbaseClient> couchbaseclients =
            new HashMap<String, CouchbaseClient>();
    /** nodes array. */
    //private ArrayList<URI> nodes = new ArrayList<URI>();
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
        try {
            String handler = "Client" + couchbaseclients.size();
            List<URI> hosts = Arrays.asList(new URI(url.toJava()));
            CouchbaseClient client = new CouchbaseClient(hosts,
            bucket.toJava(), password.toJava());
            couchbaseclients.put(handler, client);
            return Str.get(handler);
        } catch (Exception ex) {
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
     * Check if the string is json format or not. if not throw exception
     * @param doc
     * @throws QueryException
     */
    private void checkJson(final Str doc) throws QueryException {
        try {
            new FNJson(null, null, Function._JSON_PARSE,
                    doc).item(queryContext, null);
        } catch (Exception e) {
            throw new QueryException("document is not in json format");
        }
    }
    /**
     * add new document.
     * @param handler Database handler
     * @param key
     * @param value
     * @throws QueryException
     */
    public Item add(final Str handler, final Str key, final Str doc)
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
    public Item set(final Str handler, final Str key,  final Str doc)
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
    public Item replace(final Str handler, final Str key, final Str doc)
            throws QueryException {
       return put(handler, key, doc, "replace");
    }
    /**
     * Append documnt with key. 
     * @param handler
     * @param key
     * @param doc
     * @return Item
     * @throws QueryException
     */
    public Item append(final Str handler, final Str key, final Str doc)
            throws QueryException {
       return put(handler, key, doc, null);
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
    public Item put(final Str handler, final Str key, final Str doc,
            final String type) throws QueryException {
        CouchbaseClient client = getClient(handler);
        OperationFuture<Boolean> result = null;
       checkJson(doc);
        try {
            if(type != null) {
                if(type.equals("add")) {
                   client.add(
                            key.toJava(), doc.toJava());
                } else if(type.equals("replace")) {
                   client.replace(
                            key.toJava(), doc.toJava());
                } else if(type.equals("set")) {
                   result = client.set(
                            key.toJava(), doc.toJava());
                } else {
                   result = client.append(
                            key.toJava(), doc.toJava());
                }
            } else {
                result = client.append(
                        key.toJava(), doc.toJava());
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
    public Item get(final Str handler, final Str key) throws QueryException {
        CouchbaseClient client = getClient(handler);
        try {
            Object result =  client.get(key.toJava());
            if(result != null) {
                 Str json = Str.get((String) result);
                try {
                    return new FNJson(null, null, Function._JSON_PARSE,
                            json).item(queryContext, null);
                } catch (Exception e) {
                    throw new QueryException("The result is not in json Format");
                }
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
         CouchbaseClient client = getClient(handler);
         try {
             if(options.size() < 1) {
                 throw new QueryException("key set is empty");
             }
             System.out.println(options.size());
             List<String> keys = new ArrayList<String>();
             for (Value v: options) {
                String s = (String) v.toJava();
                keys.add(s);
             }
//             java.util.Map<String, Object> s = client.getBulk(keys);
//             Object s1 = client.getBulk(keys);
//             return new FNJson(null, Function._JSON_PARSE, Str.get(s2)).
//                     item(context, null);
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
    @SuppressWarnings("rawtypes")
    public Item createView(final Str handler, final Str doc, final Str viewName,
            final Str map, final Str reduce) throws QueryException {
        CouchbaseClient client = getClient(handler);
        if(map == null) {
            throw new QueryException("map function is empty");
        }
        try {
            DesignDocument<?> designDoc = new DesignDocument(doc.toJava());
            ViewDesign viewDesign;
            if(reduce != null) {
               viewDesign = new ViewDesign(viewName.toJava(),
                       map.toJava(), reduce.toJava());
            } else {
                viewDesign = new ViewDesign(viewName.toJava(), map.toJava());
            }
            designDoc.getViews().add(viewDesign);
            client.createDesignDoc(designDoc);
        } catch (Exception e) {
            throw new QueryException(e);
        }
        return null;
    }
    /**
     * 
     * @param handler
     * @param doc
     * @param viewName
     * @return
     * @throws QueryException
     */
    public Item getview(final Str handler, final Str doc, final Str viewName)
            throws QueryException {
        return getview(handler, doc, viewName, null, null);
    }
    /**
     * Get data from view.
     * @param handler
     * @param doc
     * @param viewName
     * @param mode is development or production
     * @return
     * @throws QueryException
     */
    public Item getview(final Str handler, final Str doc, final Str viewName,
            final Str mode)
            throws QueryException {
        return getview(handler, doc, viewName, mode, null);
    }
    /**
     * 
     * @param handler
     * @param doc
     * @param viewName
     * @param mode
     * @param options options like limit and so on(not completed)
     * @return
     * @throws QueryException
     */
    public Item getview(final Str handler, final Str doc, final Str viewName,
            final Str mode, final Str options) throws QueryException {
        if(mode != null) {
            System.setProperty("viewmode", mode.toJava());
        }
        CouchbaseClient client = getClient(handler);
        try {
            View view = client.getView(doc.toJava(), viewName.toJava());
            Query q = new Query();
            q.setIncludeDocs(true).setLimit(10);
            q.setStale(Stale.FALSE);
            ViewResponse response = client.query(view, q);
            java.util.Map<String, Object> map = response.getMap();
//            for(ViewRow row: response) {
//                System.out.println(row.getDocument());
//            }
            Str json = mapToJson(map);
            return new FNJson(null, null, Function._JSON_PARSE, json).
                    item(queryContext, null);
        } catch (Exception e) {
            throw new QueryException(e);
        }
        //return null;
    }
    /**
     * Convert java Map<String, Object> into json format String and then return
     * in Str.
     * @param map
     * @return
     */
    private Str mapToJson(final java.util.Map<String, Object> map) {
        final StringBuilder json = new StringBuilder();
        json.append("{ ");
        for(final Entry<String, Object> e : map.entrySet()) {
          if(json.length() > 2) json.append(", ");
          json.append('"').append(e.getKey()).append('"').append(" : ").
          append(e.getValue());
        }
        json.append(" } ");
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