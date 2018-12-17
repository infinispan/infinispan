package org.infinispan.persistence.sifs.configuration;

import static org.infinispan.persistence.sifs.configuration.Element.DATA;
import static org.infinispan.persistence.sifs.configuration.Element.INDEX;
import static org.infinispan.persistence.sifs.configuration.Element.SOFT_INDEX_FILE_STORE;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.NestingAttributeSerializer;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.cache.AbstractSegmentedStoreConfiguration;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.persistence.sifs.SoftIndexFileStore;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@BuiltBy(SoftIndexFileStoreConfigurationBuilder.class)
@ConfigurationFor(SoftIndexFileStore.class)
@SerializedWith(SoftIndexFileStoreSerializer.class)
public class SoftIndexFileStoreConfiguration extends AbstractSegmentedStoreConfiguration<SoftIndexFileStoreConfiguration> implements ConfigurationInfo {

   private static AttributeSerializer UNDER_DATA = new NestingAttributeSerializer(DATA.getLocalName());
   private static AttributeSerializer UNDER_INDEX = new NestingAttributeSerializer(INDEX.getLocalName());

   public static final AttributeDefinition<String> DATA_LOCATION = AttributeDefinition.builder("dataLocation", "Infinispan-SoftIndexFileStore-Data").immutable().autoPersist(false).serializer(UNDER_DATA).xmlName("path").build();
   public static final AttributeDefinition<String> INDEX_LOCATION = AttributeDefinition.builder("indexLocation", "Infinispan-SoftIndexFileStore-Index").immutable().serializer(UNDER_INDEX).autoPersist(false).xmlName("path").build();
   public static final AttributeDefinition<Integer> INDEX_SEGMENTS = AttributeDefinition.builder("indexSegments", 3).immutable().serializer(UNDER_INDEX).autoPersist(false).xmlName("segments").build();
   public static final AttributeDefinition<Integer> MAX_FILE_SIZE = AttributeDefinition.builder("maxFileSize", 16 * 1024 * 1024).immutable().serializer(UNDER_DATA).autoPersist(false).build();
   public static final AttributeDefinition<Integer> MIN_NODE_SIZE = AttributeDefinition.builder("minNodeSize", 0).immutable().serializer(UNDER_INDEX).autoPersist(false).build();
   public static final AttributeDefinition<Integer> MAX_NODE_SIZE = AttributeDefinition.builder("maxNodeSize", 4096).immutable().serializer(UNDER_INDEX).autoPersist(false).build();
   public static final AttributeDefinition<Integer> INDEX_QUEUE_LENGTH = AttributeDefinition.builder("indexQueueLength", 1000).immutable().serializer(UNDER_INDEX).autoPersist(false).xmlName("max-queue-length").build();
   public static final AttributeDefinition<Boolean> SYNC_WRITES = AttributeDefinition.builder("syncWrites", false).immutable().serializer(UNDER_DATA).autoPersist(false).build();
   public static final AttributeDefinition<Integer> OPEN_FILES_LIMIT = AttributeDefinition.builder("openFilesLimit", 1000).immutable().build();
   public static final AttributeDefinition<Double> COMPACTION_THRESHOLD = AttributeDefinition.builder("compactionThreshold", 0.5d).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SoftIndexFileStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), DATA_LOCATION, INDEX_LOCATION, INDEX_SEGMENTS, MAX_FILE_SIZE,
            MIN_NODE_SIZE, MAX_NODE_SIZE, INDEX_QUEUE_LENGTH, SYNC_WRITES, OPEN_FILES_LIMIT, COMPACTION_THRESHOLD);
   }

   public static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(SOFT_INDEX_FILE_STORE.getLocalName());


   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public SoftIndexFileStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
   }

   @Override
   public SoftIndexFileStoreConfiguration newConfigurationFrom(int segment) {
      AttributeSet set = SoftIndexFileStoreConfiguration.attributeDefinitionSet();
      set.read(attributes);
      String dataLocation = set.attribute(DATA_LOCATION).get();
      set.attribute(DATA_LOCATION).set(fileLocationTransform(dataLocation, segment));
      String indexLocation = set.attribute(INDEX_LOCATION).get();
      set.attribute(INDEX_LOCATION).set(fileLocationTransform(indexLocation, segment));
      return new SoftIndexFileStoreConfiguration(set.protect(), async(), singletonStore());
   }

   public String dataLocation() {
      return attributes.attribute(DATA_LOCATION).get();
   }

   public String indexLocation() {
      return attributes.attribute(INDEX_LOCATION).get();
   }

   public int indexSegments() {
      return attributes.attribute(INDEX_SEGMENTS).get();
   }

   public int maxFileSize() {
      return attributes.attribute(MAX_FILE_SIZE).get();
   }

   public int minNodeSize() {
      return attributes.attribute(MIN_NODE_SIZE).get();
   }

   public int maxNodeSize() {
      return attributes.attribute(MAX_NODE_SIZE).get();
   }

   public int indexQueueLength() {
      return attributes.attribute(INDEX_QUEUE_LENGTH).get();
   }

   public boolean syncWrites() {
      return attributes.attribute(SYNC_WRITES).get();
   }

   public int openFilesLimit() {
      return attributes.attribute(OPEN_FILES_LIMIT).get();
   }

   public double compactionThreshold() {
      return attributes.attribute(COMPACTION_THRESHOLD).get();
   }

}
