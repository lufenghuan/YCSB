package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.Iterator;
import java.lang.StringBuffer;
import java.nio.charset.Charset;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.builders.RiakObjectBuilder;
import com.basho.riak.client.raw.RawClient;
import com.basho.riak.client.raw.RiakResponse;
import com.basho.riak.client.raw.pbc.PBClientAdapter;
import com.basho.riak.client.util.CharsetUtils;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;


/**
 * riak client for YCSB
 */

public class RiakClient extends DB {

  private static final int OK = 0;
  private static final int ERROR = -1;
  private static final int SERVER_ERROR = 1;
  private static final int CLIENT_ERROR = 2;
  private boolean debug = false;

  ObjectMapper om = new ObjectMapper();
  
  private PBClientAdapter pbClient;
  
  private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");
  private static final String CONTENT_TYPE_JSON_UTF8 = "application/json;charset=UTF-8";

  private static Logger logger = Logger.getLogger(RiakClient.class);

  public RiakClient() {}

  /**
   * Initialize any state for this DB. Called once per DB instance; there is
   * one DB instance per client thread.
   */
  public void init() throws DBException {
    BasicConfigurator.configure();
    logger.setLevel(Level.INFO);  
    logger.debug("int");

    String clusterHost = getProperties().getProperty("riak.clusterHost","localhost");
    try{  
      pbClient = new PBClientAdapter(clusterHost,8087);
    }catch (IOException e){
      logger.error(e.getMessage());
      System.exit(1);
    }
  }

  @Override
  public int read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    logger.debug("readkey: " + key + " from table: " + table);
    try {
      RiakResponse response = pbClient.fetch(table, key);
      if(response.hasValue()) {
        IRiakObject obj = response.getRiakObjects()[0];
        riakObjToJson(obj, fields, result);
      }
    } catch(Exception e) {
      e.printStackTrace();
      return ERROR;
    }
    return OK;
  }

  @Override
  public int scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    logger.debug("scan " + recordcount + " records from key: " + startkey + " on table: " + table);
    //riak not support scan, need secondary index
    return OK;
  }

  @Override
  public int update(String table, String key, HashMap<String, ByteIterator> values) {
    logger.debug("updatekey: " + key + " from table: " + table);
    /* Riak not support partial fetch or update */
    try {
      RiakResponse response = pbClient.fetch(table, key);
      if(response.hasValue()) {
        IRiakObject obj = response.getRiakObjects()[0];
        byte[] data = updateJson(obj, values);
        RiakObjectBuilder builder =
          RiakObjectBuilder.newBuilder(table, key)
          .withContentType(CONTENT_TYPE_JSON_UTF8)
          .withValue(data)
          .withVClock(response.getVclock());
        pbClient.store(builder.build());
      }
    } catch (Exception e) {
      e.printStackTrace();
      return ERROR;
    }

    return OK; 
  }

  @Override
  public int insert(String table, String key,HashMap<String, ByteIterator> values) {
    logger.debug("insertkey: " + key + " from table: " + table);
    RiakObjectBuilder builder =
      RiakObjectBuilder.newBuilder(table, key)
      .withContentType(CONTENT_TYPE_JSON_UTF8);
    try {
      byte[] rawValue = jsonToBytes(values);
      pbClient.store(builder.withValue(rawValue).build());
    } catch (Exception e) {
      e.printStackTrace();
      return ERROR;
    }
    return OK;
  }

  @Override
  public int delete(String table, String key) {
    logger.debug("deletekey: " + key + " from table: " + table);
    try {
      pbClient.delete(table, key);
    } catch (IOException e) {
      e.printStackTrace();
      return ERROR;
    }
    return OK;
  }

  /**
   * Convert given <String, ByteIterator> pairs into ObjectNode,
   * then convert to byte[]
   */
  protected  byte[] jsonToBytes(Map<String, ByteIterator> values) {
    ObjectNode objNode = om.createObjectNode();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      objNode.put(entry.getKey(), entry.getValue().toString());
    }
    return objNode.toString().getBytes(CHARSET_UTF8);
  }

  /**
   * Convert the given fetched IRiakObject into JSON
   * and convert to <String, ByteIterator> pairs 
   */
  protected  void riakObjToJson(IRiakObject object, Set<String> fields, Map<String, ByteIterator> result)
    throws IOException {
    String contentType = object.getContentType();
    Charset charSet = CharsetUtils.getCharset(contentType);
    byte[] data = object.getValue();
    String dataInCharset = CharsetUtils.asString(data, charSet);
    JsonNode jsonNode = om.readTree(dataInCharset);
    
    if(fields != null) {
      // return a subset of all available fields in the json node
      for(String field: fields) {
        JsonNode f = jsonNode.get(field);
        result.put(field, new StringByteIterator(f.toString()));
      }
    } else {
      // no fields specified, just return them all
      Iterator<Map.Entry<String, JsonNode>> jsonFields = jsonNode.getFields();
      while(jsonFields.hasNext()) {
        Map.Entry<String, JsonNode> field = jsonFields.next();
        result.put(field.getKey(), new StringByteIterator(field.getValue().toString()));
      }
    }
  }

  /**
   * Helper function for update
   */
  private byte[] updateJson(IRiakObject object, Map<String, ByteIterator> values) throws IOException {
    String contentType = object.getContentType();
    Charset charSet = CharsetUtils.getCharset(contentType);
    byte[] data = object.getValue();
    String dataInCharset = CharsetUtils.asString(data, charSet);
    JsonNode jsonNode = om.readTree(dataInCharset);
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      ((ObjectNode) jsonNode).put(entry.getKey(), entry.getValue().toString());
    }
    return jsonNode.toString().getBytes(CHARSET_UTF8);
  }

}
