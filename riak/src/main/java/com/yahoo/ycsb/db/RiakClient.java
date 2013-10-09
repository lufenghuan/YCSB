/*

 * Copyright 2012 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.lang.StringBuffer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.BasicConfigurator;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.workloads.CoreWorkload;

import com.basho.riak.client.*;
import com.basho.riak.client.bucket.*;


/**
 * DynamoDB v1.3.14 client for YCSB
 */

public class RiakClient extends DB {

    private static final int OK = 0;
    private static final int SERVER_ERROR = 1;
    private static final int CLIENT_ERROR = 2;
    private String primaryKeyName;
    private boolean debug = false;
    private boolean consistentRead = false;

    private IRiakClient client;
    private Bucket myBucket;

    private int maxConnects = 50;
    
    private final int FIELD_LENGTH = 
      Integer.parseInt(CoreWorkload.FIELD_LENGTH_PROPERTY_DEFAULT); 
    private final int FIELD_COUNT = 
      Integer.parseInt(CoreWorkload.FIELD_COUNT_PROPERTY_DEFAULT);

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
      try{  
        client = RiakFactory.pbcClient();
        myBucket = client.fetchBucket("riak-benchmark-ycsb").execute();
      }catch (RiakException e){
        logger.error(e.getMessage());
        System.exit(1);
      }
    }

    @Override
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {

        logger.debug("readkey: " + key + " from table: " + table);
        return OK;
    }

    @Override
    public int scan(String table, String startkey, int recordcount,
        Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        logger.debug("scan " + recordcount + " records from key: " + startkey + " on table: " + table);
        
        return OK;
    }

    @Override
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        logger.debug("updatekey: " + key + " from table: " + table);

        return OK;
    }

    @Override
    public int insert(String table, String key,HashMap<String, ByteIterator> values) {
        logger.debug("insertkey: " + primaryKeyName + "-" + key + " from table: " + table);
        // Riak's bucket interface only support store String or Object
        StringBuffer buf = new StringBuffer(FIELD_LENGTH*FIELD_COUNT);
        try{
         // Bucket myBucket = client.fetchBucket(table).execute();
          //ByteBuffer buf = ByteBuffer.allocateDirect(FIELD_LENGTH*FIELD_COUNT);
          /*
          for (Entry<String, ByteIterator> val : values.entrySet()) {
            buf.append(val.getValue().toString());
          }
          */
          //myBucket.store(key,buf.toString()).execute();
          myBucket.store(key,key).execute();
        }catch(RiakException e){
          logger.error(e.getMessage());
          return CLIENT_ERROR;
        }
        return OK;
    }

    @Override
    public int delete(String table, String key) {
        logger.debug("deletekey: " + key + " from table: " + table);
        return OK;
    }


}
