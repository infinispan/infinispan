package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Self;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.persistence.jdbc.TableManipulation;
import org.infinispan.persistence.jdbc.logging.Log;

/**
 * TableManipulationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class TableManipulationConfigurationBuilder<B extends AbstractJdbcStoreConfigurationBuilder<?, B>, S extends TableManipulationConfigurationBuilder<B, S>> extends
                                                                                                                                                                          AbstractJdbcStoreConfigurationChildBuilder<B> implements Builder<TableManipulationConfiguration>, Self<S> {
   private static final Log log = LogFactory.getLog(TableManipulationConfigurationBuilder.class, Log.class);
   private int batchSize = TableManipulation.DEFAULT_BATCH_SIZE;
   private int fetchSize = TableManipulation.DEFAULT_FETCH_SIZE;
   private boolean createOnStart = true;
   private boolean dropOnExit = false;
   private String cacheName;
   private String idColumnName;
   private String idColumnType;
   private String dataColumnName;
   private String dataColumnType;
   private String timestampColumnName;
   private String timestampColumnType;

   // Needs package access for validate() in JdbcMixedCacheStoreConfigurationBuilder
   String tableNamePrefix;

   TableManipulationConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, B> builder) {
      super(builder);
   }

   /**
    * Repetitive DB operations this are batched according to this parameter. This is an optional parameter, and if it
    * is not specified it will be defaulted to {@link TableManipulation#DEFAULT_BATCH_SIZE}.
    */
   public S batchSize(int batchSize) {
      this.batchSize = batchSize;
      return self();
   }

   /**
    * For DB queries the fetch size is on {@link java.sql.ResultSet#setFetchSize(int)}. This is optional
    * parameter, if not specified will be defaulted to {@link TableManipulation#DEFAULT_FETCH_SIZE}.
    */
   public S fetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return self();
   }

   /**
    * Sets the prefix for the name of the table where the data will be stored. "_<cache name>" will
    * be appended to this prefix in order to enforce unique table names for each cache.
    */
   public S tableNamePrefix(String tableNamePrefix) {
      this.tableNamePrefix = tableNamePrefix;
      return self();
   }

   /**
    * Determines whether database tables should be created by the store on startup
    */
   public S createOnStart(boolean createOnStart) {
      this.createOnStart = createOnStart;
      return self();
   }

   /**
    * Determines whether database tables should be dropped by the store on shutdown
    */
   public S dropOnExit(boolean dropOnExit) {
      this.dropOnExit = dropOnExit;
      return self();
   }

   /**
    * The name of the database column used to store the keys
    */
   public S idColumnName(String idColumnName) {
      this.idColumnName = idColumnName;
      return self();
   }

   /**
    * The type of the database column used to store the keys
    */
   public S idColumnType(String idColumnType) {
      this.idColumnType = idColumnType;
      return self();
   }

   /**
    * The name of the database column used to store the entries
    */
   public S dataColumnName(String dataColumnName) {
      this.dataColumnName = dataColumnName;
      return self();
   }

   /**
    * The type of the database column used to store the entries
    */
   public S dataColumnType(String dataColumnType) {
      this.dataColumnType = dataColumnType;
      return self();
   }

   /**
    * The name of the database column used to store the timestamps
    */
   public S timestampColumnName(String timestampColumnName) {
      this.timestampColumnName = timestampColumnName;
      return self();
   }

   /**
    * The type of the database column used to store the timestamps
    */
   public S timestampColumnType(String timestampColumnType) {
      this.timestampColumnType = timestampColumnType;
      return self();
   }

   @Override
   public void validate() {
      validateIfSet("idColumnName", idColumnName);
      validateIfSet("idColumnType", idColumnType);
      validateIfSet("dataColumnName", dataColumnName);
      validateIfSet("dataColumnType", dataColumnType);
      validateIfSet("timestampColumnName", timestampColumnName);
      validateIfSet("timestampColumnType", timestampColumnType);
      validateIfSet("tableNamePrefix", tableNamePrefix);
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   private void validateIfSet(String name, String value) {
      if(value == null || value.isEmpty()) {
         throw log.tableManipulationAttributeNotSet(name);
      }
   }

   @Override
   public TableManipulationConfiguration create() {
      return new TableManipulationConfiguration(idColumnName, idColumnType, tableNamePrefix, cacheName, dataColumnName, dataColumnType, timestampColumnName, timestampColumnType,
               fetchSize, batchSize, createOnStart, dropOnExit);
   }

   @Override
   public Builder<?> read(TableManipulationConfiguration template) {
      this.batchSize = template.batchSize();
      this.fetchSize = template.fetchSize();
      this.createOnStart = template.createOnStart();
      this.dropOnExit = template.dropOnExit();
      this.idColumnName = template.idColumnName();
      this.idColumnType = template.idColumnType();
      this.dataColumnName = template.dataColumnName();
      this.dataColumnType = template.dataColumnType();
      this.timestampColumnName = template.timestampColumnName();
      this.timestampColumnType = template.timestampColumnType();
      this.cacheName = template.cacheName();
      this.tableNamePrefix = template.tableNamePrefix();

      return this;
   }
}
