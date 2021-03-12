package org.infinispan.persistence.sifs.configuration;

import static org.infinispan.configuration.cache.AbstractStoreConfiguration.SEGMENTED;
import static org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration.COMPACTION_THRESHOLD;
import static org.infinispan.persistence.sifs.configuration.SoftIndexFileStoreConfiguration.OPEN_FILES_LIMIT;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalStateConfiguration;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.sifs.Log;
import org.infinispan.persistence.sifs.SoftIndexFileStore;
import org.infinispan.util.logging.LogFactory;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class SoftIndexFileStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<SoftIndexFileStoreConfiguration, SoftIndexFileStoreConfigurationBuilder> implements ConfigurationBuilderInfo {
   private static final Log log = LogFactory.getLog(SoftIndexFileStoreConfigurationBuilder.class, Log.class);
   private static boolean NOTIFIED_SEGMENTED;

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

   /**
    * The path where the Soft-Index store will keep its data files. Under this location the store will create
    * a directory named after the cache name, under which a <code>data</code> directory will be created.
    *
    * The default behaviour is to use the {@link GlobalStateConfiguration#persistentLocation()}.
    */
   public SoftIndexFileStoreConfigurationBuilder dataLocation(String dataLocation) {
      data.dataLocation(dataLocation);
      return this;
   }

   /**
    * The path where the Soft-Index store will keep its index files. Under this location the store will create
    * a directory named after the cache name, under which a <code>index</code> directory will be created.
    *
    * The default behaviour is to use the {@link GlobalStateConfiguration#persistentLocation()}.
    */
   public SoftIndexFileStoreConfigurationBuilder indexLocation(String indexLocation) {
      index.indexLocation(indexLocation);
      return this;
   }

   /**
    * Number of index segment files. Increasing this value improves throughput but requires more threads to be spawned.
    *
    * Defaults to <code>3</code>.
    */
   public SoftIndexFileStoreConfigurationBuilder indexSegments(int indexSegments) {
      index.indexSegments(indexSegments);
      return this;
   }

   /**
    * Sets the maximum size of single data file with entries, in bytes.
    *
    * Defaults to <code>16777216</code> (16MB).
    */
   public SoftIndexFileStoreConfigurationBuilder maxFileSize(int maxFileSize) {
      data.maxFileSize(maxFileSize);
      return this;
   }

   /**
    * If the size of the node (continuous block on filesystem used in index implementation) drops below this threshold,
    * the node will try to balance its size with some neighbour node, possibly causing join of multiple nodes.
    *
    * Defaults to <code>0</code>.
    */
   public SoftIndexFileStoreConfigurationBuilder minNodeSize(int minNodeSize) {
      index.minNodeSize(minNodeSize);
      return this;
   }

   /**
    * Max size of node (continuous block on filesystem used in index implementation), in bytes.
    *
    * Defaults to <code>4096</code>.
    */
   public SoftIndexFileStoreConfigurationBuilder maxNodeSize(int maxNodeSize) {
      index.maxNodeSize(maxNodeSize);
      return this;
   }

   /**
    * Sets the maximum number of entry writes that are waiting to be written to the index, per index segment.
    *
    * Defaults to <code>1000</code>.
    */
   public SoftIndexFileStoreConfigurationBuilder indexQueueLength(int indexQueueLength) {
      index.indexQueueLength(indexQueueLength);
      return this;
   }

   /**
    * Sets whether writes shoud wait to be fsynced to disk.
    *
    * Defaults to <code>false</code>.
    */
   public SoftIndexFileStoreConfigurationBuilder syncWrites(boolean syncWrites) {
      data.syncWrites(syncWrites);
      return this;
   }

   /**
    * Sets the maximum number of open files.
    *
    * Defaults to <code>1000</code>.
    */
   public SoftIndexFileStoreConfigurationBuilder openFilesLimit(int openFilesLimit) {
      attributes.attribute(OPEN_FILES_LIMIT).set(openFilesLimit);
      return this;
   }

   /**
    * If the amount of unused space in some data file gets above this threshold, the file is compacted - entries from that file are copied to a new file and the old file is deleted.
    *
    * Defaults to <code>0.5</code> (50%).
    */
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
      Attribute<Boolean> segmentedAttribute = attributes.attribute(SEGMENTED);
      if ((!segmentedAttribute.isModified() || segmentedAttribute.get()) && !NOTIFIED_SEGMENTED) {
         NOTIFIED_SEGMENTED = true;
         org.infinispan.util.logging.Log.CONFIG.segmentedStoreUsesManyFileDescriptors(SoftIndexFileStore.class.getSimpleName());
      }
      super.validate(skipClassChecks);
      index.validate();
      double compactionThreshold = attributes.attribute(COMPACTION_THRESHOLD).get();
      if (compactionThreshold <= 0 || compactionThreshold > 1) {
         throw log.invalidCompactionThreshold(compactionThreshold);
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      PersistenceUtil.validateGlobalStateStoreLocation(globalConfig, SoftIndexFileStore.class.getSimpleName(),
            data.attributes().attribute(DataConfiguration.DATA_LOCATION),
            index.attributes().attribute(IndexConfiguration.INDEX_LOCATION));

      super.validate(globalConfig);
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
