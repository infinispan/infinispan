package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.util.Util;
import org.infinispan.persistence.jdbc.DatabaseType;

public class TableManipulationConfiguration {
   private final String idColumnName;
   private final String idColumnType;
   private final String tableNamePrefix;
   private final String cacheName;
   private final String dataColumnName;
   private final String dataColumnType;
   private final String timestampColumnName;
   private final String timestampColumnType;
   private final int fetchSize;
   private final int batchSize;
   private final boolean createOnStart;
   private final boolean dropOnExit;
   private final DatabaseType databaseType;

   TableManipulationConfiguration(String idColumnName, String idColumnType, String tableNamePrefix, String cacheName,
         String dataColumnName, String dataColumnType, String timestampColumnName, String timestampColumnType,
         DatabaseType databaseType, int fetchSize, int batchSize, boolean createOnStart, boolean dropOnExit) {
      this.idColumnName = idColumnName;
      this.idColumnType = idColumnType;
      this.tableNamePrefix = tableNamePrefix;
      this.cacheName = cacheName;
      this.dataColumnName = dataColumnName;
      this.dataColumnType = dataColumnType;
      this.timestampColumnName = timestampColumnName;
      this.timestampColumnType = timestampColumnType;
      this.databaseType = databaseType;
      this.batchSize = Util.findNextHighestPowerOfTwo(batchSize);
      this.fetchSize = fetchSize;
      this.createOnStart = createOnStart;
      this.dropOnExit = dropOnExit;
   }

   public boolean createOnStart() {
      return createOnStart;
   }

   public boolean dropOnExit() {
      return dropOnExit;
   }

   public DatabaseType databaseType() {
      return databaseType;
   }

   public String idColumnName() {
      return idColumnName;
   }

   public String idColumnType() {
      return idColumnType;
   }

   public String tableNamePrefix() {
      return tableNamePrefix;
   }

   public String cacheName() {
      return cacheName;
   }

   public String dataColumnName() {
      return dataColumnName;
   }

   public String dataColumnType() {
      return dataColumnType;
   }

   public String timestampColumnName() {
      return timestampColumnName;
   }

   public String timestampColumnType() {
      return timestampColumnType;
   }

   public int fetchSize() {
      return fetchSize;
   }

   /**
    * @return the size of batches to process.  Guaranteed to be a power of two.
    */
   public int batchSize() {
      return batchSize;
   }

   @Override
   public String toString() {
      return "TableManipulationConfiguration [idColumnName=" + idColumnName + ", idColumnType=" + idColumnType
            + ", tableNamePrefix=" + tableNamePrefix + ", cacheName=" + cacheName + ", dataColumnName="
            + dataColumnName + ", dataColumnType=" + dataColumnType + ", timestampColumnName=" + timestampColumnName
            + ", timestampColumnType=" + timestampColumnType + ", fetchSize=" + fetchSize + ", batchSize=" + batchSize
            + "]";
   }


}