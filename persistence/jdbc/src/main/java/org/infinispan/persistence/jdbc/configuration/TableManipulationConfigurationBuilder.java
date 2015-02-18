package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Self;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.persistence.jdbc.TableManipulation;
import org.infinispan.persistence.jdbc.logging.Log;

import static org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration.*;

/**
 * TableManipulationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class TableManipulationConfigurationBuilder<B extends AbstractJdbcStoreConfigurationBuilder<?, B>, S extends TableManipulationConfigurationBuilder<B, S>> extends
                                                                                                                                                                          AbstractJdbcStoreConfigurationChildBuilder<B> implements Builder<TableManipulationConfiguration>, Self<S> {
   private static final Log log = LogFactory.getLog(TableManipulationConfigurationBuilder.class, Log.class);
   private final AttributeSet attributes;

   TableManipulationConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, B> builder) {
      super(builder);
      attributes = TableManipulationConfiguration.attributeSet();
   }

   /**
    * Repetitive DB operations this are batched according to this parameter. This is an optional parameter, and if it
    * is not specified it will be defaulted to {@link TableManipulation#DEFAULT_BATCH_SIZE}.
    */
   public S batchSize(int batchSize) {
      attributes.attribute(BATCH_SIZE).set(batchSize);
      return self();
   }

   /**
    * For DB queries the fetch size is on {@link java.sql.ResultSet#setFetchSize(int)}. This is optional
    * parameter, if not specified will be defaulted to {@link TableManipulation#DEFAULT_FETCH_SIZE}.
    */
   public S fetchSize(int fetchSize) {
      attributes.attribute(FETCH_SIZE).set(fetchSize);
      return self();
   }

   /**
    * Sets the prefix for the name of the table where the data will be stored. "_<cache name>" will
    * be appended to this prefix in order to enforce unique table names for each cache.
    */
   public S tableNamePrefix(String tableNamePrefix) {
      attributes.attribute(TABLE_NAME_PREFIX).set(tableNamePrefix);
      return self();
   }

   String tableNamePrefix() {
      return attributes.attribute(TABLE_NAME_PREFIX).get();
   }

   /**
    * Determines whether database tables should be created by the store on startup
    */
   public S createOnStart(boolean createOnStart) {
      attributes.attribute(CREATE_ON_START).set(createOnStart);
      return self();
   }

   /**
    * Determines whether database tables should be dropped by the store on shutdown
    */
   public S dropOnExit(boolean dropOnExit) {
      attributes.attribute(DROP_ON_EXIT).set(dropOnExit);
      return self();
   }

   /**
    * The name of the database column used to store the keys
    */
   public S idColumnName(String idColumnName) {
      attributes.attribute(ID_COLUMN_NAME).set(idColumnName);
      return self();
   }

   /**
    * The type of the database column used to store the keys
    */
   public S idColumnType(String idColumnType) {
      attributes.attribute(ID_COLUMN_TYPE).set(idColumnType);
      return self();
   }

   /**
    * The name of the database column used to store the entries
    */
   public S dataColumnName(String dataColumnName) {
      attributes.attribute(DATA_COLUMN_NAME).set(dataColumnName);
      return self();
   }

   /**
    * The type of the database column used to store the entries
    */
   public S dataColumnType(String dataColumnType) {
      attributes.attribute(DATA_COLUMN_TYPE).set(dataColumnType);
      return self();
   }

   /**
    * The name of the database column used to store the timestamps
    */
   public S timestampColumnName(String timestampColumnName) {
      attributes.attribute(TIMESTAMP_COLUMN_NAME).set(timestampColumnName);
      return self();
   }

   /**
    * The type of the database column used to store the timestamps
    */
   public S timestampColumnType(String timestampColumnType) {
      attributes.attribute(TIMESTAMP_COLUMN_TYPE).set(timestampColumnType);
      return self();
   }

   @Override
   public void validate() {
      validateIfSet(ID_COLUMN_NAME, ID_COLUMN_TYPE, DATA_COLUMN_NAME, DATA_COLUMN_TYPE, TIMESTAMP_COLUMN_NAME, TIMESTAMP_COLUMN_TYPE, TABLE_NAME_PREFIX);
   }

   private void validateIfSet(AttributeDefinition<?>... definitions) {
      for(AttributeDefinition<?> definition : definitions) {
         String value = attributes.attribute(definition).asObject();
         if(value == null || value.isEmpty()) {
            throw log.tableManipulationAttributeNotSet(definition.name());
         }
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public TableManipulationConfiguration create() {
      return new TableManipulationConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(TableManipulationConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "TableManipulationConfigurationBuilder [attributes=" + attributes + "]";
   }


}
