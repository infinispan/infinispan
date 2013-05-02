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
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.infinispan.Cache;
import org.infinispan.config.ConfigurationException;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.loaders.AbstractCacheStore;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderMetadata;
import org.infinispan.loaders.keymappers.MarshallingTwoWayKey2StringMapper;
import org.infinispan.loaders.keymappers.TwoWayKey2StringMapper;
import org.infinispan.loaders.keymappers.UnsupportedKeyTypeException;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Cache store using HBase as the implementation.
 *
 * @author Justin Hayes
 * @since 5.2
 */
@CacheLoaderMetadata(configurationClass = HBaseCacheStoreConfig.class)
public class HBaseCacheStore extends AbstractCacheStore {
   private static final Log log = LogFactory.getLog(HBaseCacheStore.class, Log.class);

   private HBaseCacheStoreConfig config;
   private String cacheName;
   private TwoWayKey2StringMapper keyMapper;

   private String entryTable;
   private String entryColumnFamily;
   private String entryValueField;
   private String entryKeyPrefix;
   private String expirationTable;
   private String expirationColumnFamily;
   private String expirationValueField;
   private String expirationKeyPrefix;

   private HBaseFacade hbf;

   @Override
   public void init(CacheLoaderConfig clc, Cache<?, ?> cache, StreamingMarshaller m)
            throws CacheLoaderException {
      super.init(clc, cache, m);
      this.cacheName = cache.getName();
      this.config = (HBaseCacheStoreConfig) clc;
   }

   @Override
   public void start() throws CacheLoaderException {
      log.debug("In HBaseCacheStore.start");
      try {
         // config for entries
         entryTable = config.entryTable;
         entryColumnFamily = config.entryColumnFamily;
         entryValueField = config.entryValueField;
         entryKeyPrefix = "e_" + (config.isSharedTable() ? cacheName + "_" : "");

         // config for expiration
         expirationTable = config.expirationTable;
         expirationKeyPrefix = "x_" + (config.isSharedTable() ? "_" + cacheName : "");
         expirationColumnFamily = config.expirationColumnFamily;
         expirationValueField = config.expirationValueField;

         keyMapper = (TwoWayKey2StringMapper) Util.getInstance(config.getKeyMapper(),
                  config.getClassLoader());
         if(keyMapper instanceof MarshallingTwoWayKey2StringMapper) {
            ((MarshallingTwoWayKey2StringMapper)keyMapper).setMarshaller(getMarshaller());
         }

         Map<String, String> props = new HashMap<String, String>();
         props.put("hbase.zookeeper.quorum", config.hbaseZookeeperQuorum);
         props.put("hbase.zookeeper.property.clientPort", Integer.toString(config.hbaseZookeeperPropertyClientPort));
         hbf = new HBaseFacade(props);
      } catch (Exception e) {
         throw new ConfigurationException(e);
      }

      // create the cache store table if necessary
      if (config.autoCreateTable) {
         log.infof("Automatically creating %s and %s tables.", this.entryTable,
                  this.expirationTable);
         // create required HBase structures (table and column families) for the cache
         try {
            List<String> colFamilies = Collections.singletonList(this.entryColumnFamily);

            // column family should only support a max of 1 version
            hbf.createTable(this.entryTable, colFamilies, 1);
         } catch (HBaseException ex) {
            if (ex.getCause() instanceof TableExistsException) {
               log.infof("Not creating %s because it already exists.", this.entryTable);
            } else {
               throw new CacheLoaderException("Got HadoopException while creating the "
                        + this.entryTable + " cache store table.", ex);
            }
         }

         // create required HBase structures (table and column families) for the cache expiration
         // table
         try {
            List<String> colFamilies = Collections.singletonList(this.expirationColumnFamily);

            // column family should only support a max of 1 version
            hbf.createTable(this.expirationTable, colFamilies, 1);
         } catch (HBaseException ex) {
            if (ex.getCause() instanceof TableExistsException) {
               log.infof("Not creating %s because it already exists.", this.expirationTable);
            } else {
               throw new CacheLoaderException("Got HadoopException while creating the "
                        + this.expirationTable + " cache store table.", ex);
            }
         }
      }

      log.info("Cleaning up expired entries...");
      purgeInternal();

      log.info("HBaseCacheStore started");
      super.start();
   }

   /**
    * Stores an entry into the cache. If this entry can expire, it also adds a row to the expiration
    * table so we can purge it later on after it has expired.
    *
    * @param entry
    *           the object to store in the cache
    */
   @Override
   public void store(InternalCacheEntry entry) throws CacheLoaderException {
      log.debugf("In HBaseCacheStore.store for %s: %s", this.entryTable, entry.getKey());

      Object key = entry.getKey();
      String hashedKey = hashKey(this.entryKeyPrefix, key);

      try {
         byte[] val = marshall(entry);

         Map<String, byte[]> valMap = Collections.singletonMap(entryValueField, val);

         Map<String, Map<String, byte[]>> cfMap = Collections.singletonMap(entryColumnFamily,
                  valMap);

         hbf.addRow(this.entryTable, hashedKey, cfMap);

         // Add a row to the expiration table if necessary
         if (entry.canExpire()) {
            Map<String, byte[]> expValMap = Collections.singletonMap(expirationValueField,
                     Bytes.toBytes(hashedKey));
            Map<String, Map<String, byte[]>> expCfMap = Collections.singletonMap(
                     expirationColumnFamily, expValMap);

            String expKey = "ts_" + String.valueOf(timeService.wallClockTime());
            String hashedExpKey = hashKey(this.expirationKeyPrefix, expKey);
            hbf.addRow(this.expirationTable, hashedExpKey, expCfMap);
         }
      } catch (HBaseException ex) {
         log.error("HadoopException storing entry: " + ex.getMessage());
         throw new CacheLoaderException(ex);
      } catch (Exception ex2) {
         log.error("Exception storing entry: " + ex2.getMessage());
         throw new CacheLoaderException(ex2);
      }

   }

   /**
    * Stores an object that has been unmarshalled to a stream into the cache.
    *
    * @param in
    *           the object input stream
    */
   @Override
   public void fromStream(ObjectInput in) throws CacheLoaderException {
      try {
         int count = 0;
         while (true) {
            count++;
            InternalCacheEntry entry = (InternalCacheEntry) getMarshaller().objectFromObjectStream(
                     in);
            if (entry == null) {
               break;
            }
            store(entry);
         }
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      } catch (ClassNotFoundException e) {
         throw new CacheLoaderException(e);
      } catch (InterruptedException ie) {
         if (log.isTraceEnabled()) {
            log.trace("Interrupted while reading from stream");
         }
         Thread.currentThread().interrupt();
      }
   }

   /**
    * Loads all entries from the cache and marshalls them to an object stream.
    *
    * @param out
    *           the output stream to marshall the entries to
    */
   @Override
   public void toStream(ObjectOutput out) throws CacheLoaderException {
      try {
         Set<InternalCacheEntry> loadAll = loadAll();
         int count = 0;
         for (InternalCacheEntry entry : loadAll) {
            getMarshaller().objectToObjectStream(entry, out);
            count++;
         }
         getMarshaller().objectToObjectStream(null, out);
      } catch (IOException e) {
         throw new CacheLoaderException(e);
      }
   }

   /**
    * Removes all entries from the cache. This include removing items from the expiration table.
    */
   @Override
   public void clear() throws CacheLoaderException {
      // clear both the entry table and the expiration table
      String[] tableNames = { this.entryTable, this.expirationTable };
      String[] keyPrefixes = { this.entryKeyPrefix, this.expirationKeyPrefix };

      for (int i = 0; i < tableNames.length; i++) {
         // get all keys for this table
         Set<Object> allKeys = loadAllKeysForTable(tableNames[i], null);
         Set<Object> allKeysHashed = new HashSet<Object>(allKeys.size());
         for (Object key : allKeys) {
            allKeysHashed.add(hashKey(keyPrefixes[i], key));
         }

         // remove the rows for those keys
         try {
            hbf.removeRows(tableNames[i], allKeysHashed);
         } catch (HBaseException ex) {
            log.error("Caught HadoopException clearing the " + tableNames[i] + " table: "
                     + ex.getMessage());
            throw new CacheLoaderException(ex);
         }
      }
   }

   /**
    * Removes an entry from the cache, given its key.
    *
    * @param key
    *           the key for the entry to remove.
    */
   @Override
   public boolean remove(Object key) throws CacheLoaderException {
      log.debugf("In HBaseCacheStore.remove for key %s", key);

      String hashedKey = hashKey(this.entryKeyPrefix, key);
      try {
         return hbf.removeRow(this.entryTable, hashedKey);
      } catch (HBaseException ex) {
         log.error("HadoopException removing an object from the cache: " + ex.getMessage(), ex);
         throw new CacheLoaderException("HadoopException removing an object from the cache: "
                  + ex.getMessage(), ex);
      }
   }

   /**
    * Loads an entry from the cache, given its key.
    *
    * @param key
    *           the key for the entry to load.
    */
   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      log.debugf("In HBaseCacheStore.load for key %s", key);

      String hashedKey = hashKey(this.entryKeyPrefix, key);
      List<String> colFamilies = Collections.singletonList(entryColumnFamily);

      try {
         Map<String, Map<String, byte[]>> resultMap = hbf.readRow(this.entryTable, hashedKey,
                  colFamilies);
         if (resultMap.isEmpty()) {
            log.debugf("Key %s not found.", hashedKey);
            return null;
         }

         byte[] val = resultMap.get(entryColumnFamily).get(entryValueField);

         InternalCacheEntry ice = unmarshall(val, key);
         if (ice != null && ice.isExpired(timeService.wallClockTime())) {
            remove(key);
            return null;
         }

         return ice;
      } catch (HBaseException ex) {
         log.error("Caught HadoopException: " + ex.getMessage());
         throw new CacheLoaderException(ex);
      } catch (Exception ex2) {
         log.error("Caught Exception: " + ex2.getMessage());
         throw new CacheLoaderException(ex2);
      }
   }

   /**
    * Loads all entries from the cache.
    */
   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return load(Integer.MAX_VALUE);
   }

   /**
    * Loads entries from the cache up to a certain number.
    *
    * @param numEntries
    *           the max number of entries to load.
    */
   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      log.debugf("In HBaseCacheStore.load for %s entries.", numEntries);

      Map<String, byte[]> items = null;

      try {
         items = hbf.scan(this.entryTable, numEntries, entryColumnFamily, entryValueField);
      } catch (HBaseException ex) {
         log.error("Caught HadoopException loading " + numEntries + " entries: " + ex.getMessage());
         throw new CacheLoaderException(ex);
      }

      Set<InternalCacheEntry> iceSet = new HashSet<InternalCacheEntry>(items.size());
      try {
         for (Entry<String, byte[]> entry : items.entrySet()) {
            Object unhashedKey = unhashKey(this.entryKeyPrefix, entry.getKey());
            iceSet.add(unmarshall(entry.getValue(), unhashedKey));
         }
      } catch (Exception ex) {
         log.error("Caught exception loading items: " + ex.getMessage());
         throw new CacheLoaderException(ex);
      }

      return iceSet;
   }

   /**
    * Loads all keys from the cache, optionally excluding some.
    *
    * @param keysToExclude
    *           a set of keys that should not be returned.
    */
   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      return this.loadAllKeysForTable(this.entryTable, keysToExclude);
   }

   private Set<Object> loadAllKeysForTable(String table, Set<Object> keysToExclude)
            throws CacheLoaderException {
      log.debugf("In HBaseCacheStore.loadAllKeys for %s", table);

      Set<Object> allKeys = null;
      try {
         allKeys = hbf.scanForKeys(table);
      } catch (HBaseException ex) {
         log.error("HadoopException loading all keys: " + ex.getMessage());
         throw new CacheLoaderException(ex);
      }

      // unhash the keys
      String keyPrefix = table.equals(this.entryTable) ? this.entryKeyPrefix
               : this.expirationKeyPrefix;
      Set<Object> unhashedKeys = new HashSet<Object>(allKeys.size());
      for (Object hashedKey : allKeys) {
         unhashedKeys.add(unhashKey(keyPrefix, hashedKey));
      }

      // now filter keys if necessary
      if (keysToExclude != null) {
         unhashedKeys.removeAll(keysToExclude);
      }

      return unhashedKeys;
   }

   /**
    * Returns the class that represents this cache store's configuration.
    */
   @Override
   public Class<? extends CacheLoaderConfig> getConfigurationClass() {
      return HBaseCacheStoreConfig.class;
   }

   /**
    * Purges any expired entries from the cache.
    */
   @Override
   protected void purgeInternal() throws CacheLoaderException {
      log.debug("Purging expired entries.");

      try {
         // query the expiration table to find out the entries that have been expired
         long ts = timeService.wallClockTime();
         Map<String, Map<String, Map<String, byte[]>>> rowsToPurge = hbf.readRows(
                  this.expirationTable, this.expirationKeyPrefix, ts, this.expirationColumnFamily,
                  this.expirationValueField);

         Set<Object> keysToDelete = new HashSet<Object>();
         Set<Object> expKeysToDelete = new HashSet<Object>();

         // figure out the cache entry keys for the entries that have expired
         for (Entry<String, Map<String, Map<String, byte[]>>> entry : rowsToPurge.entrySet()) {
            expKeysToDelete.add(entry.getKey());
            byte[] targetKeyBytes = entry.getValue().get(this.expirationColumnFamily)
                     .get(this.expirationValueField);
            String targetKey = Bytes.toString(targetKeyBytes);
            keysToDelete.add(targetKey);
         }

         // remove the entries that have expired
         if (keysToDelete.size() > 0) {
            hbf.removeRows(this.entryTable, keysToDelete);
         }

         // now remove any expiration rows with timestamps before now
         hbf.removeRows(this.expirationTable, expKeysToDelete);
      } catch (HBaseException ex) {
         log.error("HadoopException loading all keys: " + ex.getMessage());
         throw new CacheLoaderException(ex);
      }
   }

   @Override
   public String toString() {
      return "HBaseCacheStore";
   }

   private String hashKey(String keyPrefix, Object key) throws UnsupportedKeyTypeException {
      if (key == null) {
         return "";
      }
      if (key != null && !keyMapper.isSupportedType(key.getClass())) {
         throw new UnsupportedKeyTypeException(key);
      }

      return keyPrefix + keyMapper.getStringMapping(key);
   }

   private Object unhashKey(String keyPrefix, Object key) {
      String skey = new String(key.toString());
      if (skey.startsWith(keyPrefix)) {
         return keyMapper.getKeyMapping(skey.substring(keyPrefix.length()));
      } else {
         return null;
      }
   }

   private byte[] marshall(InternalCacheEntry entry) throws IOException, InterruptedException {
      return getMarshaller().objectToByteBuffer(entry.toInternalCacheValue());
   }

   private InternalCacheEntry unmarshall(Object o, Object key) throws IOException,
            ClassNotFoundException {
      if (o == null) {
         return null;
      }
      byte b[] = (byte[]) o;
      InternalCacheValue v = (InternalCacheValue) getMarshaller().objectFromByteBuffer(b);
      return v.toInternalCacheEntry(key);
   }

}
