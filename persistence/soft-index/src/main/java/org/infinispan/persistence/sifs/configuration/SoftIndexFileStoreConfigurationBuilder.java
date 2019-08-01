package org.infinispan.persistence.sifs.configuration;

import static org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration.COMPACTION_THRESHOLD;
import static org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration.OPEN_FILES_LIMIT;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.sifs.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class SoftIndexFileStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<SoftIndexFileStoreConfiguration, SoftIndexFileStoreConfigurationBuilder> implements ConfigurationBuilderInfo {
   private static final Log log = LogFactory.getLog(SoftIndexFileStoreConfigurationBuilder.class, Log.class);

   private final IndexConfigurationBuilder index = new IndexConfigurationBuilder();
   private final DataConfigurationBuilder data = new DataConfigurationBuilder();
   private final List<ConfigurationBuilderInfo> builders;

   public SoftIndexFileStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, SoftIndexFileStoreConfiguration.attributeDefinitionSet());
      builders = Arrays.asList(index, data);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return SoftIndexFileStoreConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return builders;
   }

   public SoftIndexFileStoreConfigurationBuilder dataLocation(String dataLocation) {
      data.dataLocation(dataLocation);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder indexLocation(String indexLocation) {
      index.indexLocation(indexLocation);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder indexSegments(int indexSegments) {
      index.indexSegments(indexSegments);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder maxFileSize(int maxFileSize) {
      data.maxFileSize(maxFileSize);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder minNodeSize(int minNodeSize) {
      index.minNodeSize(minNodeSize);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder maxNodeSize(int maxNodeSize) {
      index.maxNodeSize(maxNodeSize);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder indexQueueLength(int indexQueueLength) {
      index.indexQueueLength(indexQueueLength);
      return this;
   }
   public SoftIndexFileStoreConfigurationBuilder syncWrites(boolean syncWrites) {
      data.syncWrites(syncWrites);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder openFilesLimit(int openFilesLimit) {
      attributes.attribute(OPEN_FILES_LIMIT).set(openFilesLimit);
      return this;
   }

   public SoftIndexFileStoreConfigurationBuilder compactionThreshold(double compactionThreshold) {
      attributes.attribute(COMPACTION_THRESHOLD).set(compactionThreshold);
      return this;
   }

   @Override
   public SoftIndexFileStoreConfiguration create() {
      return new SoftIndexFileStoreConfiguration(attributes.protect(), async.create(), index.create(), data.create());
   }

   @Override
   public Builder<?> read(SoftIndexFileStoreConfiguration template) {
      super.read(template);
      index.read(template.index());
      data.read(template.data());
      return this;
   }

   @Override
   public SoftIndexFileStoreConfigurationBuilder self() {
      return this;
   }

   @Override
   protected void validate(boolean skipClassChecks) {
      super.validate(skipClassChecks);
      index.validate();
      double compactionThreshold = attributes.attribute(COMPACTION_THRESHOLD).get();
      if (compactionThreshold <= 0 || compactionThreshold > 1) {
         throw log.invalidCompactionThreshold(compactionThreshold);
      }
   }

   @Override
   public String toString() {
      return "SoftIndexFileStoreConfigurationBuilder{" +
            "index=" + index +
            ", data=" + data +
            ", attributes=" + attributes +
            ", async=" + async +
            '}';
   }
}
