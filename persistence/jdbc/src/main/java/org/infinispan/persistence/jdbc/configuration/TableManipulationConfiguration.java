package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.persistence.jdbc.TableManipulation;

public class TableManipulationConfiguration {
   static final AttributeDefinition<String> ID_COLUMN_NAME = AttributeDefinition.builder("idColumnName", null, String.class).immutable().build();
   static final AttributeDefinition<String> ID_COLUMN_TYPE = AttributeDefinition.builder("idColumnType", null, String.class).immutable().build();
   static final AttributeDefinition<String> TABLE_NAME_PREFIX = AttributeDefinition.builder("tableNamePrefix", null, String.class).immutable().build();
   static final AttributeDefinition<String> CACHE_NAME = AttributeDefinition.builder("cacheName", null, String.class).immutable().build();
   static final AttributeDefinition<String> DATA_COLUMN_NAME = AttributeDefinition.builder("dataColumnName", null, String.class).immutable().build();
   static final AttributeDefinition<String> DATA_COLUMN_TYPE = AttributeDefinition.builder("dataColumnType", null, String.class).immutable().build();
   static final AttributeDefinition<String> TIMESTAMP_COLUMN_NAME = AttributeDefinition.builder("timestampColumnName", null, String.class).immutable().build();
   static final AttributeDefinition<String> TIMESTAMP_COLUMN_TYPE = AttributeDefinition.builder("timestampColumnType", null, String.class).immutable().build();
   static final AttributeDefinition<Integer> BATCH_SIZE = AttributeDefinition.builder("batchSize", TableManipulation.DEFAULT_BATCH_SIZE).immutable().build();
   static final AttributeDefinition<Integer> FETCH_SIZE = AttributeDefinition.builder("fetchSize", TableManipulation.DEFAULT_FETCH_SIZE).immutable().build();
   static final AttributeDefinition<Boolean> CREATE_ON_START = AttributeDefinition.builder("createOnStart", true).immutable().build();
   static final AttributeDefinition<Boolean> DROP_ON_EXIT = AttributeDefinition.builder("dropOnExit", false).immutable().build();

   static AttributeSet attributeSet() {
      return new AttributeSet(TableManipulationConfiguration.class, ID_COLUMN_NAME, ID_COLUMN_TYPE, TABLE_NAME_PREFIX, CACHE_NAME, DATA_COLUMN_NAME, DATA_COLUMN_TYPE,
            TIMESTAMP_COLUMN_NAME, TIMESTAMP_COLUMN_TYPE, BATCH_SIZE, FETCH_SIZE, CREATE_ON_START, DROP_ON_EXIT);
   }

   private final Attribute<String> idColumnName;
   private final Attribute<String> idColumnType;
   private final Attribute<String> tableNamePrefix;
   private final Attribute<String> cacheName;
   private final Attribute<String> dataColumnName;
   private final Attribute<String> dataColumnType;
   private final Attribute<String> timestampColumnName;
   private final Attribute<String> timestampColumnType;
   private final Attribute<Integer> batchSize;
   private final Attribute<Integer> fetchSize;
   private final Attribute<Boolean> createOnStart;
   private final Attribute<Boolean> dropOnExit;
   private final AttributeSet attributes;

   TableManipulationConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      idColumnName = attributes.attribute(ID_COLUMN_NAME);
      idColumnType = attributes.attribute(ID_COLUMN_TYPE);
      tableNamePrefix = attributes.attribute(TABLE_NAME_PREFIX);
      cacheName = attributes.attribute(CACHE_NAME);
      dataColumnName = attributes.attribute(DATA_COLUMN_NAME);
      dataColumnType = attributes.attribute(DATA_COLUMN_TYPE);
      timestampColumnName = attributes.attribute(TIMESTAMP_COLUMN_NAME);
      timestampColumnType = attributes.attribute(TIMESTAMP_COLUMN_TYPE);
      batchSize = attributes.attribute(BATCH_SIZE);
      fetchSize = attributes.attribute(FETCH_SIZE);
      createOnStart = attributes.attribute(CREATE_ON_START);
      dropOnExit = attributes.attribute(DROP_ON_EXIT);
   }

   public boolean createOnStart() {
      return createOnStart.get();
   }

   public boolean dropOnExit() {
      return dropOnExit.get();
   }

   public String idColumnName() {
      return idColumnName.get();
   }

   public String idColumnType() {
      return idColumnType.get();
   }

   public String tableNamePrefix() {
      return tableNamePrefix.get();
   }

   public String cacheName() {
      return cacheName.get();
   }

   public String dataColumnName() {
      return dataColumnName.get();
   }

   public String dataColumnType() {
      return dataColumnType.get();
   }

   public String timestampColumnName() {
      return timestampColumnName.get();
   }

   public String timestampColumnType() {
      return timestampColumnType.get();
   }

   public int fetchSize() {
      return fetchSize.get();
   }

   /**
    * @return the size of batches to process.  Guaranteed to be a power of two.
    */
   public int batchSize() {
      return batchSize.get();
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "TableManipulationConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      TableManipulationConfiguration other = (TableManipulationConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }
}
