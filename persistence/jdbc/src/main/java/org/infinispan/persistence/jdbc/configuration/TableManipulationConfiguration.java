package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.Element.DATA_COLUMN;
import static org.infinispan.persistence.jdbc.configuration.Element.ID_COLUMN;
import static org.infinispan.persistence.jdbc.configuration.Element.STRING_KEYED_TABLE;
import static org.infinispan.persistence.jdbc.configuration.Element.TIMESTAMP_COLUMN;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.NestingAttributeSerializer;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.persistence.jdbc.table.management.TableManager;

public class TableManipulationConfiguration implements ConfigurationInfo {

   private static final AttributeSerializer<String, ?, ?> UNDER_ID = new NestingAttributeSerializer<>(ID_COLUMN.getLocalName());
   private static final AttributeSerializer<String, ?, ?> UNDER_DATA = new NestingAttributeSerializer<>(DATA_COLUMN.getLocalName());
   private static final AttributeSerializer<String, ?, ?> UNDER_TIMESTAMP = new NestingAttributeSerializer<>(TIMESTAMP_COLUMN.getLocalName());

   public static final AttributeDefinition<String> ID_COLUMN_NAME = AttributeDefinition.builder("idColumnName", null, String.class).serializer(UNDER_ID).xmlName("name").immutable().build();
   public static final AttributeDefinition<String> ID_COLUMN_TYPE = AttributeDefinition.builder("idColumnType", null, String.class).immutable().serializer(UNDER_ID).xmlName("type").build();
   public static final AttributeDefinition<String> TABLE_NAME_PREFIX = AttributeDefinition.builder("tableNamePrefix", null, String.class).xmlName("prefix").immutable().build();
   public static final AttributeDefinition<String> CACHE_NAME = AttributeDefinition.builder("cacheName", null, String.class).immutable().build();
   public static final AttributeDefinition<String> DATA_COLUMN_NAME = AttributeDefinition.builder("dataColumnName", null, String.class).serializer(UNDER_DATA).xmlName("name").immutable().build();
   public static final AttributeDefinition<String> DATA_COLUMN_TYPE = AttributeDefinition.builder("dataColumnType", null, String.class).immutable().serializer(UNDER_DATA).xmlName("type").build();
   public static final AttributeDefinition<String> TIMESTAMP_COLUMN_NAME = AttributeDefinition.builder("timestampColumnName", null, String.class).serializer(UNDER_TIMESTAMP).xmlName("name").immutable().build();
   public static final AttributeDefinition<String> TIMESTAMP_COLUMN_TYPE = AttributeDefinition.builder("timestampColumnType", null, String.class).serializer(UNDER_TIMESTAMP).xmlName("type").immutable().build();
   public static final AttributeDefinition<String> SEGMENT_COLUMN_NAME = AttributeDefinition.builder("segmentColumnName", null, String.class).immutable().build();
   public static final AttributeDefinition<String> SEGMENT_COLUMN_TYPE = AttributeDefinition.builder("segmentColumnType", null, String.class).immutable().build();
   // TODO remove in 10.0
   public static final AttributeDefinition<Integer> BATCH_SIZE = AttributeDefinition.builder("batchSize", AbstractStoreConfiguration.MAX_BATCH_SIZE.getDefaultValue()).immutable().build();
   public static final AttributeDefinition<Integer> FETCH_SIZE = AttributeDefinition.builder("fetchSize", TableManager.DEFAULT_FETCH_SIZE).immutable().build();
   public static final AttributeDefinition<Boolean> CREATE_ON_START = AttributeDefinition.builder("createOnStart", true).immutable().build();
   public static final AttributeDefinition<Boolean> DROP_ON_EXIT = AttributeDefinition.builder("dropOnExit", false).immutable().build();

   static AttributeSet attributeSet() {
      return new AttributeSet(TableManipulationConfiguration.class, ID_COLUMN_NAME, ID_COLUMN_TYPE, TABLE_NAME_PREFIX,
            CACHE_NAME, DATA_COLUMN_NAME, DATA_COLUMN_TYPE, TIMESTAMP_COLUMN_NAME, TIMESTAMP_COLUMN_TYPE,
            SEGMENT_COLUMN_NAME, SEGMENT_COLUMN_TYPE, BATCH_SIZE, FETCH_SIZE, CREATE_ON_START, DROP_ON_EXIT);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(STRING_KEYED_TABLE.getLocalName());

   private final Attribute<String> idColumnName;
   private final Attribute<String> idColumnType;
   private final Attribute<String> tableNamePrefix;
   private final Attribute<String> cacheName;
   private final Attribute<String> dataColumnName;
   private final Attribute<String> dataColumnType;
   private final Attribute<String> timestampColumnName;
   private final Attribute<String> timestampColumnType;
   private final Attribute<String> segmentColumnName;
   private final Attribute<String> segmentColumnType;
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
      segmentColumnName = attributes.attribute(SEGMENT_COLUMN_NAME);
      segmentColumnType = attributes.attribute(SEGMENT_COLUMN_TYPE);
      batchSize = attributes.attribute(BATCH_SIZE);
      fetchSize = attributes.attribute(FETCH_SIZE);
      createOnStart = attributes.attribute(CREATE_ON_START);
      dropOnExit = attributes.attribute(DROP_ON_EXIT);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
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

   public String segmentColumnName() {
      return segmentColumnName.get();
   }

   public String segmentColumnType() {
      return segmentColumnType.get();
   }

   public int fetchSize() {
      return fetchSize.get();
   }

   /**
    * @deprecated please use {@link org.infinispan.configuration.cache.AbstractStoreConfiguration#maxBatchSize()} instead.
    * @return the size of batches to process.  Guaranteed to be a power of two.
    */
   @Deprecated
   public int batchSize() {
      return batchSize.get();
   }

   public AttributeSet attributes() {
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
