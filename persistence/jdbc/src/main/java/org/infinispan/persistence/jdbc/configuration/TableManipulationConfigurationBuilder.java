package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration.BATCH_SIZE;
import static org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration.CREATE_ON_START;
import static org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration.DROP_ON_EXIT;
import static org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration.FETCH_SIZE;
import static org.infinispan.persistence.jdbc.configuration.TableManipulationConfiguration.TABLE_NAME_PREFIX;
import static org.infinispan.persistence.jdbc.logging.Log.CONFIG;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.Self;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * TableManipulationConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public abstract class TableManipulationConfigurationBuilder<B extends AbstractJdbcStoreConfigurationBuilder<?, B>, S extends TableManipulationConfigurationBuilder<B, S>>
      extends AbstractJdbcStoreConfigurationChildBuilder<B>
      implements Builder<TableManipulationConfiguration>, Self<S>, ConfigurationBuilderInfo {
   private final AttributeSet attributes;
   private final DataColumnConfigurationBuilder dataColumn = new DataColumnConfigurationBuilder();
   private final IdColumnConfigurationBuilder idColumn = new IdColumnConfigurationBuilder();
   private final TimestampColumnConfigurationBuilder timeStampColumn = new TimestampColumnConfigurationBuilder();
   private final SegmentColumnConfigurationBuilder segmentColumn;
   private List<ConfigurationBuilderInfo> subElements;

   TableManipulationConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, B> builder) {
      super(builder);
      attributes = TableManipulationConfiguration.attributeSet();
      segmentColumn = new SegmentColumnConfigurationBuilder(builder);
      subElements = Arrays.asList(idColumn, dataColumn, timeStampColumn, segmentColumn);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return TableManipulationConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return subElements;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * @deprecated Please use {@link org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder#maxBatchSize(int)} instead.
    */
   @Deprecated
   public S batchSize(int batchSize) {
      attributes.attribute(BATCH_SIZE).set(batchSize);
      maxBatchSize(batchSize);
      return self();
   }

   /**
    * For DB queries the fetch size is on {@link java.sql.ResultSet#setFetchSize(int)}. This is optional
    * parameter, if not specified will be defaulted to {@link TableManager#DEFAULT_FETCH_SIZE}.
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
      idColumn.idColumnName(idColumnName);
      return self();
   }

   /**
    * The type of the database column used to store the keys
    */
   public S idColumnType(String idColumnType) {
      idColumn.idColumnType(idColumnType);
      return self();
   }

   /**
    * The name of the database column used to store the entries
    */
   public S dataColumnName(String dataColumnName) {
      dataColumn.dataColumnName(dataColumnName);
      return self();
   }

   /**
    * The type of the database column used to store the entries
    */
   public S dataColumnType(String dataColumnType) {
      dataColumn.dataColumnType(dataColumnType);
      return self();
   }

   /**
    * The name of the database column used to store the timestamps
    */
   public S timestampColumnName(String timestampColumnName) {
      timeStampColumn.dataColumnName(timestampColumnName);
      return self();
   }

   /**
    * The type of the database column used to store the timestamps
    */
   public S timestampColumnType(String timestampColumnType) {
      timeStampColumn.dataColumnType(timestampColumnType);
      return self();
   }

   /**
    * The name of the database column used to store the segments
    */
   public S segmentColumnName(String segmentColumnName) {
      segmentColumn.columnName(segmentColumnName);
      return self();
   }

   /**
    * The type of the database column used to store the segments
    */
   public S segmentColumnType(String segmentColumnType) {
      segmentColumn.columnType(segmentColumnType);
      return self();
   }

   @Override
   public void validate() {
      validateIfSet(attributes, TABLE_NAME_PREFIX);
      idColumn.validate();
      dataColumn.validate();
      timeStampColumn.validate();
      segmentColumn.validate();
   }

   static void validateIfSet(AttributeSet attributes, AttributeDefinition<?>... definitions) {
      for(AttributeDefinition<?> definition : definitions) {
         String value = attributes.attribute(definition).asObject();
         if(value == null || value.isEmpty()) {
            throw CONFIG.tableManipulationAttributeNotSet(definition.name());
         }
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public TableManipulationConfiguration create() {
      return new TableManipulationConfiguration(attributes.protect(), idColumn.create(), dataColumn.create(), timeStampColumn.create(), segmentColumn.create());
   }

   @Override
   public Builder<?> read(TableManipulationConfiguration template) {
      attributes.read(template.attributes());
      idColumn.read(template.idColumnConfiguration());
      dataColumn.read(template.dataColumnConfiguration());
      timeStampColumn.read(template.timeStampColumnConfiguration());
      segmentColumn.read(template.segmentColumnConfiguration());
      return this;
   }

   @Override
   public String toString() {
      return "TableManipulationConfigurationBuilder [attributes=" + attributes + "]";
   }


}
