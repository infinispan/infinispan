/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.hbase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Adapter class for HBase. Provides a logical abstraction on top of the basic HBase API that makes
 * it easier to use.
 *
 * @author Justin Hayes
 * @since 5.2
 */
public class HBaseFacade {
   private static final Log log = LogFactory.getLog(HBaseFacade.class, Log.class);

   private static final int SCAN_BATCH_SIZE = 100;

   private static Configuration CONFIG;

   /**
    * Create a new HBaseService.
    */
   public HBaseFacade() {
      CONFIG = HBaseConfiguration.create();
   }

   /**
    * Create a new HBaseService.
    */
   public HBaseFacade(Map<String, String> props) {
      this();
      for (Entry<String, String> prop : props.entrySet()) {
         CONFIG.set(prop.getKey(), prop.getValue());
      }
   }

   private void close(HBaseAdmin admin) {
      if(admin!=null) {
         try {
            admin.close();
         } catch (IOException e) {
         }
      }
   }

   /**
    * Creates a new HBase table.
    *
    * @param name
    *           the name of the table
    * @param columnFamilies
    *           a list of column family names to use
    * @throws HBaseException
    */
   public void createTable(String name, List<String> columnFamilies) throws HBaseException {
      createTable(name, columnFamilies, HColumnDescriptor.DEFAULT_VERSIONS);
   }

   /**
    * Creates a new HBase table.
    *
    * @param name
    *           the name of the table
    * @param columnFamilies
    *           a list of column family names to create
    * @param maxVersions
    *           the max number of versions to maintain for the column families
    * @throws HBaseException
    */
   public void createTable(String name, List<String> columnFamilies, int maxVersions)
            throws HBaseException {
      if (name == null || "".equals(name)) {
         throw new HBaseException("Table name must not be empty.");
      }
      HBaseAdmin admin = null;
      try {
         admin = new HBaseAdmin(CONFIG);

         HTableDescriptor desc = new HTableDescriptor(name.getBytes());

         // add all column families
         if (columnFamilies != null) {
            for (String cf : columnFamilies) {
               HColumnDescriptor colFamilyDesc = new HColumnDescriptor(cf.getBytes());
               colFamilyDesc.setMaxVersions(maxVersions);
               desc.addFamily(colFamilyDesc);
            }
         }

         int retries = 0;
         do {
            try {
              admin.createTable(desc);
              return;
            } catch (PleaseHoldException e) {
               TimeUnit.SECONDS.sleep(1);
               retries++;
            }
         } while(retries < 10);
         admin.createTable(desc);
      } catch (Exception ex) {
         throw new HBaseException("Exception occurred when creating HBase table.", ex);
      } finally {
         close(admin);
      }
   }

   /**
    * Deletes a HBase table.
    *
    * @param name
    *           the name of the table
    * @throws HBaseException
    */
   public void deleteTable(String name) throws HBaseException {
      if (name == null || "".equals(name)) {
         throw new HBaseException("Table name must not be empty.");
      }

      HBaseAdmin admin = null;
      try {
         admin = new HBaseAdmin(CONFIG);

         admin.disableTable(name);
         admin.deleteTable(name);
      } catch (Exception ex) {
         throw new HBaseException("Exception occurred when deleting HBase table.", ex);
      } finally {
         close(admin);
      }
   }

   /**
    * Checks to see if a table exists.
    *
    * @param name
    *           the name of the table
    * @throws HBaseException
    */
   public boolean tableExists(String name) throws HBaseException {
      if (name == null || "".equals(name)) {
         throw new HBaseException("Table name must not be empty.");
      }

      HBaseAdmin admin = null;
      try {
         admin = new HBaseAdmin(CONFIG);

         return admin.isTableAvailable(name);
      } catch (Exception ex) {
         throw new HBaseException("Exception occurred when deleting HBase table.", ex);
      } finally {
         close(admin);
      }
   }

   /**
    * Adds a row to a HBase table.
    *
    * @param tableName
    *           the table to add to
    * @param key
    *           the unique key for the row
    * @param dataMap
    *           the data to add, where the outer map's keys are column family name and values are
    *           maps that contain the fields and values to add into that column family.
    * @throws HBaseException
    */
   public void addRow(String tableName, String key, Map<String, Map<String, byte[]>> dataMap)
            throws HBaseException {
      if (tableName == null || "".equals(tableName)) {
         throw new HBaseException("Table name must not be empty.");
      }
      if (isEmpty(key)) {
         throw new IllegalArgumentException("key cannot be null or empty.");
      }
      if (isEmpty(dataMap)) {
         throw new IllegalArgumentException("dataMap cannot be null or empty.");
      }

      log.debugf("Writing %s data values to table %s and key %s.", dataMap.size(), tableName, key);

      // write data to HBase, going column family by column family, and field by field
      HTable table = null;
      try {
         table = new HTable(CONFIG, tableName);
         Put p = new Put(Bytes.toBytes(key));
         for (Entry<String, Map<String, byte[]>> columFamilyEntry : dataMap.entrySet()) {
            String cfName = columFamilyEntry.getKey();
            Map<String, byte[]> cfDataCells = columFamilyEntry.getValue();
            for (Entry<String, byte[]> dataCellEntry : cfDataCells.entrySet()) {
               p.add(Bytes.toBytes(cfName), Bytes.toBytes(dataCellEntry.getKey()),
                        Bytes.toBytes(ByteBuffer.wrap(dataCellEntry.getValue())));
            }
         }
         table.put(p);
      } catch (IOException ex) {
         throw new HBaseException("Exception happened while " + "writing row to HBase.", ex);
      } finally {
         try {
            table.close();
         } catch (Exception ex) {
            // do nothing
         }
      }
   }

   /**
    * Reads the values in a row from a table.
    *
    * @param tableName
    *           the table to read from
    * @param key
    *           the key for the row
    * @param columnFamilies
    *           which column families to return
    * @return a nested map where the outer map's keys are column family name and values are maps
    *         that contain the fields and values from that column family.
    * @throws HBaseException
    */
   public Map<String, Map<String, byte[]>> readRow(String tableName, String key,
            List<String> columnFamilies) throws HBaseException {
      if (tableName == null || "".equals(tableName)) {
         throw new HBaseException("Table name must not be empty.");
      }
      if (isEmpty(key)) {
         throw new IllegalArgumentException("key cannot be null or empty.");
      }
      if (isEmpty(columnFamilies)) {
         throw new IllegalArgumentException("columnFamilies cannot be null or empty.");
      }

      log.debugf("Reading row from table %s and key %s.", tableName, key);

      // read data from HBase
      HTable table = null;
      try {
         table = new HTable(CONFIG, tableName);
         Get g = new Get(Bytes.toBytes(key));
         Result result = table.get(g);
         Map<String, Map<String, byte[]>> resultMap = new HashMap<String, Map<String, byte[]>>(
                  columnFamilies.size());

         // bail if we didn't get any results
         if (result.isEmpty()) {
            return resultMap;
         }

         // extract the fields and values from all column families
         for (String cfName : columnFamilies) {
            Map<String, byte[]> cfDataMap = new HashMap<String, byte[]>();

            Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(cfName));
            for (Entry<byte[], byte[]> familyMapEntry : familyMap.entrySet()) {
               cfDataMap.put(new String(familyMapEntry.getKey()), familyMapEntry.getValue());
            }

            resultMap.put(cfName, cfDataMap);
         }

         return resultMap;
      } catch (IOException ex) {
         throw new HBaseException("Exception happened while reading row from HBase.", ex);
      } finally {
         try {
            table.close();
         } catch (Exception ex) {
            // do nothing
         }
      }
   }

   /**
    * Reads the values from multiple rows from a table, using a key prefix and a timestamp. For
    * example, if rows were added with keys: key1 key2 key3 key4 Then readRows("myTable", "key", 3,
    * ...) would return the data for rows with key1, key1, and key3.
    *
    * @param tableName
    *           the table to read from
    * @param keyPrefix
    *           the key prefix to use for the query
    * @param ts
    *           timestamp before which rows should be returned
    * @param columnFamily
    *           which column family to return
    * @param qualifier
    *           which qualifier (ie field) to return
    * @return a nested map where the outermost map's keys are row keys and values are map whose keys
    *         are column family name and values are maps that contain the fields and values from
    *         that column family for that row.
    * @throws HBaseException
    */
   public Map<String, Map<String, Map<String, byte[]>>> readRows(String tableName,
            String keyPrefix, long ts, String columnFamily, String qualifier) throws HBaseException {
      if (tableName == null || "".equals(tableName)) {
         throw new HBaseException("Table name must not be empty.");
      }
      if (isEmpty(keyPrefix)) {
         throw new IllegalArgumentException("keyPrefix cannot be null or empty.");
      }
      if (isEmpty(columnFamily)) {
         throw new IllegalArgumentException("columnFamily cannot be null or empty.");
      }

      log.debugf("Reading rows from table %s with key prefix %s and ts %s", tableName, keyPrefix,
               ts);

      // read data from HBase
      HTable table = null;
      ResultScanner scanner = null;
      try {
         table = new HTable(CONFIG, tableName);

         Scan s = new Scan();
         s.setMaxVersions(Integer.MAX_VALUE);
         s.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
         s.setStartRow(Bytes.toBytes(keyPrefix));
         s.setStopRow(Bytes.toBytes(keyPrefix + ts));

         Map<String, Map<String, Map<String, byte[]>>> resultMaps = new HashMap<String, Map<String, Map<String, byte[]>>>();

         // scan the table in batches to improve performance
         scanner = table.getScanner(s);
         Result[] resultBatch = scanner.next(SCAN_BATCH_SIZE);

         while (resultBatch != null && resultBatch.length > 0) {
            for (int i = 0; i < resultBatch.length; i++) {
               // extract the data for this row
               if (!resultBatch[i].isEmpty()) {
                  List<KeyValue> kv = resultBatch[i].getColumn(Bytes.toBytes(columnFamily),
                           Bytes.toBytes(qualifier));

                  Map<String, byte[]> resultCFMap = new HashMap<String, byte[]>();
                  for (KeyValue keyValue : kv) {
                     resultCFMap.put(qualifier, keyValue.getValue());
                  }

                  Map<String, Map<String, byte[]>> resultMap = Collections.singletonMap(
                           columnFamily, resultCFMap);

                  resultMaps.put(getKeyFromResult(resultBatch[i]), resultMap);
               }
            }

            // get the next batch
            resultBatch = scanner.next(SCAN_BATCH_SIZE);
         }

         return resultMaps;
      } catch (IOException ex) {
         throw new HBaseException("Exception happened while reading rows from HBase.", ex);
      } finally {
         try {
            table.close();
            scanner.close();
         } catch (Exception ex) {
            // do nothing
         }
      }
   }

   /**
    * Removes a row from a table.
    *
    * @param tableName
    *           the table to remove from
    * @param key
    *           the key for the row to remove
    * @return true if the row existed and was deleted; false if it didn't exist.
    * @throws HBaseException
    */
   public boolean removeRow(String tableName, String key) throws HBaseException {
      if (tableName == null || "".equals(tableName)) {
         throw new HBaseException("Table name must not be empty.");
      }
      if (isEmpty(key)) {
         throw new IllegalArgumentException("key cannot be null or empty.");
      }

      log.debugf("Removing row from table %s with key %s.", tableName, key);

      // remove data from HBase
      HTable table = null;
      try {
         table = new HTable(CONFIG, tableName);

         // check to see if it exists first
         Get get = new Get(Bytes.toBytes(key));
         boolean exists = table.exists(get);

         if (exists) {
            Delete d = new Delete(Bytes.toBytes(key));
            table.delete(d);
         }

         return exists;
      } catch (IOException ex) {
         throw new HBaseException("Exception happened while " + "deleting row from HBase.", ex);
      } finally {
         try {
            table.close();
         } catch (Exception ex) {
            // do nothing
         }
      }
   }

   /**
    * Removes rows from a table.
    *
    * @param tableName
    *           the table to remove from
    * @param keys
    *           a list of keys for the row to remove
    * @throws HBaseException
    */
   public void removeRows(String tableName, Set<Object> keys) throws HBaseException {
      if (tableName == null || "".equals(tableName)) {
         throw new HBaseException("Table name must not be empty.");
      }
      if (isEmpty(keys)) {
         throw new IllegalArgumentException("keys cannot be null or empty.");
      }

      log.debugf("Removing rows from table %s.", tableName);

      // remove data from HBase
      HTable table = null;
      try {
         table = new HTable(CONFIG, tableName);

         List<Delete> deletes = new ArrayList<Delete>(keys.size());
         for (Object key : keys) {
            deletes.add(new Delete(Bytes.toBytes((String) key)));
         }

         table.delete(deletes);
      } catch (IOException ex) {
         throw new HBaseException("Exception happened while " + "deleting rows from HBase.", ex);
      } finally {
         try {
            table.close();
         } catch (Exception ex) {
            // do nothing
         }
      }
   }

   /**
    * Scans an entire table, returning the values from the given column family and field for each
    * row.
    *
    * TODO - maybe update to accept multiple column families and fields and return a Map<String,
    * Map<String, Map<String, byte[]>>>
    *
    * @param tableName
    *           the table to scan
    * @param columnFamily
    *           the column family of the field to return
    * @param qualifier
    *           the field to return
    * @return map mapping row keys to values for all rows
    * @throws HBaseException
    */
   public Map<String, byte[]> scan(String tableName, String columnFamily, String qualifier)
            throws HBaseException {
      return scan(tableName, Integer.MAX_VALUE, columnFamily, qualifier);
   }

   /**
    * Scans an entire table, returning the values from the given column family and field for each
    * row.
    *
    * TODO - maybe update to accept multiple column families and fields and return a Map<String,
    * Map<String, Map<String, byte[]>>>
    *
    * @param tableName
    *           the table to scan
    * @param numEntries
    *           the max number of entries to return; if < 0, defaults to Integer.MAX_VALUE
    * @param columnFamily
    *           the column family of the field to return
    * @param qualifier
    *           the field to return
    * @return map mapping row keys to values for all rows
    * @throws HBaseException
    */
   public Map<String, byte[]> scan(String tableName, int numEntries, String columnFamily,
            String qualifier) throws HBaseException {
      if (isEmpty(tableName)) {
         throw new IllegalArgumentException("tableName cannot be null or empty.");
      }
      if (isEmpty(columnFamily)) {
         throw new IllegalArgumentException("colFamily cannot be null or empty.");
      }
      if (isEmpty(qualifier)) {
         throw new IllegalArgumentException("field cannot be null or empty.");
      }

      // default to maximum number of entries if not specified
      if (numEntries < 0) {
         numEntries = Integer.MAX_VALUE;
      }

      log.debugf("Scanning table %s.", tableName);

      // read data from HBase
      ResultScanner scanner = null;
      HTable table = null;
      try {
         table = new HTable(CONFIG, tableName);
         scanner = table.getScanner(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));

         // scan the table in batches to improve performance
         Map<String, byte[]> results = new HashMap<String, byte[]>();
         int ct = 0;
         while (ct < numEntries) {
            int batchSize = Math.min(SCAN_BATCH_SIZE, numEntries - ct);
            Result[] batch = scanner.next(batchSize);

            if (batch.length <= 0) {
               break;
            } else {
               for (int i = 0; i < batch.length; i++) {
                  // extract the data for this row
                  Result curr = batch[i];

                  String key = getKeyFromResult(curr);

                  byte[] valueArr = null;
                  if (curr.containsColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier))) {
                     valueArr = curr
                              .getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(qualifier));
                     log.tracef("Value=%s", Bytes.toString(valueArr));
                  }

                  log.tracef("Added %s->%s", key, Bytes.toString(valueArr));
                  results.put(key, valueArr);
               }
               ct += batch.length;
            }
         }

         return results;
      } catch (IOException ex) {
         throw new HBaseException(
                  "Exception happened while " + "scanning table " + tableName + ".", ex);
      } catch (Exception ex) {
         throw new HBaseException(
                  "Exception happened while " + "scanning table " + tableName + ".", ex);
      } finally {
         scanner.close();
         try {
            table.close();
            scanner.close();
         } catch (Exception ex) {
            // do nothing
         }
      }
   }

   /**
    * Returns a set of all unique keys for a given table.
    *
    * @param tableName
    *           the table to return the keys for
    * @return
    * @throws HBaseException
    */
   public Set<Object> scanForKeys(String tableName) throws HBaseException {
      if (isEmpty(tableName)) {
         throw new IllegalArgumentException("tableName cannot be null or empty.");
      }

      log.debugf("Scanning table %s for keys.", tableName);

      // scan the entire table, extracting the key from each row
      ResultScanner scanner = null;
      HTable table = null;
      try {
         table = new HTable(CONFIG, tableName);
         scanner = table.getScanner(new Scan());

         Set<Object> results = new HashSet<Object>();

         // batch the scan to improve performance
         Result[] resultBatch = scanner.next(SCAN_BATCH_SIZE);
         while (resultBatch != null && resultBatch.length > 0) {
            for (int i = 0; i < resultBatch.length; i++) {
               String key = getKeyFromResult(resultBatch[i]);
               results.add(key);
            }

            // get the next batch
            resultBatch = scanner.next(SCAN_BATCH_SIZE);
         }

         return results;
      } catch (IOException ex) {
         throw new HBaseException(
                  "Exception happened while " + "scanning table " + tableName + ".", ex);
      } finally {
         scanner.close();
         try {
            table.close();
            scanner.close();
         } catch (Exception ex) {
            // do nothing
         }
      }
   }

   private String getKeyFromResult(Result result) {
      List<KeyValue> l = result.list();
      String key = null;

      // This assumes that the first keyValue will always
      // be the one that contains the actual row key. So we
      // don't need to iterate over all keyValue items.
      // We can always iterate over all KeyValues and output
      // a warning message if the key inferred for a given
      // KeyValue is different than the other KeyValues' keys.
      // This also assumes that the string representation of
      // the KeyValue has the key as the start of the string,
      // going all the way to the first "/".
      KeyValue keyValue = l.get(0);
      String keyValStr = keyValue.toString();
      int index = keyValStr.indexOf("/");
      key = keyValStr.substring(0, index);

      return key;
   }

   /**
    * Returns true if the argument is null or is "empty", which is determined based on the type of
    * the argument.
    *
    * @param o
    * @return
    */
   private boolean isEmpty(Object o) {
      if (o == null) {
         return true;
      } else if (o instanceof String && "".equals(o)) {
         return true;
      } else if (o instanceof List<?> && ((List<?>) o).size() < 1) {
         return true;
      } else if (o instanceof Map<?, ?> && ((Map<?, ?>) o).size() < 1) {
         return true;
      } else if (o instanceof byte[] && ((byte[]) o).length < 1) {
         return true;
      }

      return false;
   }

}
