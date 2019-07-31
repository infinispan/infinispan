package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.Element.STRING_KEYED_TABLE;

import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.persistence.jdbc.impl.table.TableManager;

public class TableManipulationConfiguration implements ConfigurationInfo {

   public static final AttributeDefinition<String> TABLE_NAME_PREFIX = AttributeDefinition.builder("tableNamePrefix", null, String.class).xmlName("prefix").immutable().build();
   public static final AttributeDefinition<String> CACHE_NAME = AttributeDefinition.builder("cacheName", null, String.class).immutable().build();
   // TODO remove in 10.0
   public static final AttributeDefinition<Integer> BATCH_SIZE = AttributeDefinition.builder("batchSize", AbstractStoreConfiguration.MAX_BATCH_SIZE.getDefaultValue()).immutable().build();
   public static final AttributeDefinition<Integer> FETCH_SIZE = AttributeDefinition.builder("fetchSize", TableManager.DEFAULT_FETCH_SIZE).immutable().build();
   public static final AttributeDefinition<Boolean> CREATE_ON_START = AttributeDefinition.builder("createOnStart", true).immutable().build();
   public static final AttributeDefinition<Boolean> DROP_ON_EXIT = AttributeDefinition.builder("dropOnExit", false).immutable().build();

   static AttributeSet attributeSet() {
      return new AttributeSet(TableManipulationConfiguration.class, TABLE_NAME_PREFIX, CACHE_NAME, BATCH_SIZE, FETCH_SIZE, CREATE_ON_START, DROP_ON_EXIT);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(STRING_KEYED_TABLE.getLocalName());

   private final Attribute<String> tableNamePrefix;
   private final Attribute<String> cacheName;

   private final Attribute<Integer> batchSize;
   private final Attribute<Integer> fetchSize;
   private final Attribute<Boolean> createOnStart;
   private final Attribute<Boolean> dropOnExit;
   private final AttributeSet attributes;

   private final IdColumnConfiguration idColumn;
   private final DataColumnConfiguration dataColumn;
   private final TimestampColumnConfiguration timeStamp;
   private final SegmentColumnConfiguration segmentColumn;
   private List<ConfigurationInfo> subElements;

   TableManipulationConfiguration(AttributeSet attributes,
                                  IdColumnConfiguration idColumn,
                                  DataColumnConfiguration dataColumn,
                                  TimestampColumnConfiguration timestampColumn,
                                  SegmentColumnConfiguration segmentColumn) {
      this.attributes = attributes.checkProtection();
      tableNamePrefix = attributes.attribute(TABLE_NAME_PREFIX);
      cacheName = attributes.attribute(CACHE_NAME);
      batchSize = attributes.attribute(BATCH_SIZE);
      fetchSize = attributes.attribute(FETCH_SIZE);
      createOnStart = attributes.attribute(CREATE_ON_START);
      dropOnExit = attributes.attribute(DROP_ON_EXIT);
      this.idColumn = idColumn;
      this.dataColumn = dataColumn;
      this.timeStamp = timestampColumn;
      this.segmentColumn = segmentColumn;
      subElements = Arrays.asList(idColumn, dataColumn, timestampColumn, segmentColumn);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

   public boolean createOnStart() {
      return createOnStart.get();
   }

   public boolean dropOnExit() {
      return dropOnExit.get();
   }

   public String idColumnName() {
      return idColumn.idColumnName();
   }

   public String idColumnType() {
      return idColumn.idColumnType();
   }

   public String tableNamePrefix() {
      return tableNamePrefix.get();
   }

   public String cacheName() {
      return cacheName.get();
   }

   public String dataColumnName() {
      return dataColumn.dataColumnName();
   }

   public String dataColumnType() {
      return dataColumn.dataColumnType();
   }

   public String timestampColumnName() {
      return timeStamp.dataColumnName();
   }

   public String timestampColumnType() {
      return timeStamp.dataColumnType();
   }

   public String segmentColumnName() {
      return segmentColumn.segmentColumnName();
   }

   public String segmentColumnType() {
      return segmentColumn.segmentColumnType();
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

   public IdColumnConfiguration idColumnConfiguration() {
      return idColumn;
   }

   public DataColumnConfiguration dataColumnConfiguration() {
      return dataColumn;
   }

   public TimestampColumnConfiguration timeStampColumnConfiguration() {
      return timeStamp;
   }

   public SegmentColumnConfiguration segmentColumnConfiguration() {
      return segmentColumn;
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
