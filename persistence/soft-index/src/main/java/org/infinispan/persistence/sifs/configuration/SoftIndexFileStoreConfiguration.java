package org.infinispan.persistence.sifs.configuration;

import static org.infinispan.persistence.sifs.configuration.Element.SOFT_INDEX_FILE_STORE;

import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.persistence.sifs.NonBlockingSoftIndexFileStore;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@BuiltBy(SoftIndexFileStoreConfigurationBuilder.class)
@ConfigurationFor(NonBlockingSoftIndexFileStore.class)
@SerializedWith(SoftIndexFileStoreSerializer.class)
public class SoftIndexFileStoreConfiguration extends AbstractStoreConfiguration implements ConfigurationInfo {

   public static final AttributeDefinition<Integer> OPEN_FILES_LIMIT = AttributeDefinition.builder("openFilesLimit", 1000).immutable().build();
   public static final AttributeDefinition<Double> COMPACTION_THRESHOLD = AttributeDefinition.builder("compactionThreshold", 0.5d).immutable().build();
   private final IndexConfiguration index;
   private final DataConfiguration data;
   private final List<ConfigurationInfo> elements;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SoftIndexFileStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), OPEN_FILES_LIMIT, COMPACTION_THRESHOLD);
   }

   public static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(SOFT_INDEX_FILE_STORE.getLocalName(), true, false);

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public SoftIndexFileStoreConfiguration(AttributeSet attributes,
                                          AsyncStoreConfiguration async,
                                          IndexConfiguration indexConfiguration,
                                          DataConfiguration dataConfiguration) {
      super(attributes, async);
      index = indexConfiguration;
      data = dataConfiguration;
      elements = Arrays.asList(index, data);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return elements;
   }

   public String dataLocation() {
      return data.dataLocation();
   }

   public String indexLocation() {
      return index.indexLocation();
   }

   public int indexSegments() {
      return index.indexSegments();
   }

   public int maxFileSize() {
      return data.maxFileSize();
   }

   public int minNodeSize() {
      return index.minNodeSize();
   }

   public int maxNodeSize() {
      return index.maxNodeSize();
   }

   public int indexQueueLength() {
      return index.indexQueueLength();
   }

   public boolean syncWrites() {
      return data.syncWrites();
   }

   public int openFilesLimit() {
      return attributes.attribute(OPEN_FILES_LIMIT).get();
   }

   public double compactionThreshold() {
      return attributes.attribute(COMPACTION_THRESHOLD).get();
   }

   public IndexConfiguration index() {
      return index;
   }

   public DataConfiguration data() {
      return data;
   }
}
