package org.infinispan.loaders.hbase;

import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.loaders.keymappers.MarshalledValueOrPrimitiveMapper;

/**
 * Configures {@link HBaseCacheStore}.
 */
public class HBaseCacheStoreConfig extends LockSupportCacheStoreConfig {

   /** The serialVersionUID */
   private static final long serialVersionUID = -7845734960711045535L;

   /**
    * @configRef desc="The server name of the HBase zookeeper quorum."
    */
   String hbaseZookeeperQuorum = "localhost";

   /**
    * @configRef desc="The HBase zookeeper client port."
    */
   int hbaseZookeeperPropertyClientPort = 2181;

   /**
    * @configRef desc="The HBase table for storing the cache entries"
    */
   String entryTable = "ISPNCacheStore";

   /**
    * @configRef desc="The column family for entries"
    */
   String entryColumnFamily = "E";

   /**
    * @configRef desc="The field name containing the entries"
    */
   String entryValueField = "EV";

   /**
    * @configRef desc="The HBase table for storing the cache expiration metadata"
    */
   String expirationTable = "ISPNCacheStoreExpiration";

   /**
    * @configRef desc="The column family for expirations"
    */
   String expirationColumnFamily = "X";

   /**
    * @configRef desc="The field name containing the entries"
    */
   String expirationValueField = "XV";

   /**
    * @configRef desc="Whether the table is shared between multiple caches"
    */
   boolean sharedTable = false;

   /**
    * @configRef desc=
    *            "Whether to automatically create the HBase table with the appropriate column families (true by default)"
    */
   boolean autoCreateTable = true;

   /**
    * @configRef desc=
    *            "The keymapper for converting keys to strings (uses the MarshalledValueOrPrimitiveMapper by default)"
    */
   String keyMapper = MarshalledValueOrPrimitiveMapper.class.getName();

   public HBaseCacheStoreConfig() {
      setCacheLoaderClassName(HBaseCacheStore.class.getName());
   }

   public String getHbaseZookeeperQuorum() {
      return hbaseZookeeperQuorum;
   }

   public void setHbaseZookeeperQuorum(String hbaseZookeeperQuorum) {
      this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
   }

   public int getHbaseZookeeperPropertyClientPort() {
      return hbaseZookeeperPropertyClientPort;
   }

   public void setHbaseZookeeperPropertyClientPort(int hbaseZookeeperPropertyClientPort) {
      this.hbaseZookeeperPropertyClientPort = hbaseZookeeperPropertyClientPort;
   }

   public String getEntryTable() {
      return entryTable;
   }

   public void setEntryTable(String entryTable) {
      this.entryTable = entryTable;
   }

   public String getEntryColumnFamily() {
      return entryColumnFamily;
   }

   public void setEntryColumnFamily(String entryColumnFamily) {
      this.entryColumnFamily = entryColumnFamily;
   }

   public String getEntryValueField() {
      return entryValueField;
   }

   public void setEntryValueField(String entryValueField) {
      this.entryValueField = entryValueField;
   }

   public String getExpirationTable() {
      return expirationTable;
   }

   public void setExpirationTable(String expirationTable) {
      this.expirationTable = expirationTable;
   }

   public String getExpirationColumnFamily() {
      return expirationColumnFamily;
   }

   public void setExpirationColumnFamily(String expirationColumnFamily) {
      this.expirationColumnFamily = expirationColumnFamily;
   }

   public String getExpirationValueField() {
      return expirationValueField;
   }

   public void setExpirationValueField(String expirationValueField) {
      this.expirationValueField = expirationValueField;
   }

   public boolean isSharedTable() {
      return sharedTable;
   }

   public void setSharedTable(boolean sharedTable) {
      this.sharedTable = sharedTable;
   }

   public String getKeyMapper() {
      return keyMapper;
   }

   public void setKeyMapper(String keyMapper) {
      this.keyMapper = keyMapper;
   }

   public boolean isAutoCreateTable() {
      return autoCreateTable;
   }

   public void setAutoCreateTable(boolean autoCreateTable) {
      this.autoCreateTable = autoCreateTable;
   }

}
