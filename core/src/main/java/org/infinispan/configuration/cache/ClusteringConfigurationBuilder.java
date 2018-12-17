package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.ClusteringConfiguration.BIAS_ACQUISITION;
import static org.infinispan.configuration.cache.ClusteringConfiguration.BIAS_LIFESPAN;
import static org.infinispan.configuration.cache.ClusteringConfiguration.CACHE_MODE;
import static org.infinispan.configuration.cache.ClusteringConfiguration.INVALIDATION_BATCH_SIZE;
import static org.infinispan.configuration.cache.ClusteringConfiguration.REMOTE_TIMEOUT;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Defines clustered characteristics of the cache.
 *
 * @author pmuir
 *
 */
public class ClusteringConfigurationBuilder extends AbstractConfigurationChildBuilder implements
      ClusteringConfigurationChildBuilder, Builder<ClusteringConfiguration>, ConfigurationBuilderInfo {
   private static final Log log = LogFactory.getLog(ClusteringConfigurationBuilder.class, Log.class);

   private final HashConfigurationBuilder hashConfigurationBuilder;
   private final L1ConfigurationBuilder l1ConfigurationBuilder;
   private final StateTransferConfigurationBuilder stateTransferConfigurationBuilder;
   private final PartitionHandlingConfigurationBuilder partitionHandlingConfigurationBuilder;
   private final AttributeSet attributes;
   private final List<ConfigurationBuilderInfo> subElements = new ArrayList<>();

   ClusteringConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.attributes = ClusteringConfiguration.attributeDefinitionSet();
      this.hashConfigurationBuilder = new HashConfigurationBuilder(this);
      this.l1ConfigurationBuilder = new L1ConfigurationBuilder(this);
      this.stateTransferConfigurationBuilder = new StateTransferConfigurationBuilder(this);
      this.partitionHandlingConfigurationBuilder = new PartitionHandlingConfigurationBuilder(this);
      this.subElements.addAll(Arrays.asList(hashConfigurationBuilder, l1ConfigurationBuilder, stateTransferConfigurationBuilder, partitionHandlingConfigurationBuilder));
   }

   @Override
   public ElementDefinition<? extends ConfigurationInfo> getElementDefinition() {
      return ClusteringConfiguration.ELEMENT_DEFINITION;
   }


   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return subElements;
   }

   /**
    * Cache mode. See {@link CacheMode} for information on the various cache modes available.
    */
   public ClusteringConfigurationBuilder cacheMode(CacheMode cacheMode) {
      attributes.attribute(CACHE_MODE).set(cacheMode);
      return this;
   }

   public CacheMode cacheMode() {
      return attributes.attribute(CACHE_MODE).get();
   }

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public ClusteringConfigurationBuilder remoteTimeout(long l) {
      attributes.attribute(REMOTE_TIMEOUT).set(l);
      return this;
   }

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public ClusteringConfigurationBuilder remoteTimeout(long l, TimeUnit unit) {
      return remoteTimeout(unit.toMillis(l));
   }

   /**
    * For scattered cache, the threshold after which batched invalidations are sent
    */
   public ClusteringConfigurationBuilder invalidationBatchSize(int size) {
      attributes.attribute(INVALIDATION_BATCH_SIZE).set(size);
      return this;
   }

   /**
    * Used in scattered cache. Acquired bias allows reading data on non-owner, but slows
    * down further writes from other nodes.
    */
   public ClusteringConfigurationBuilder biasAcquisition(BiasAcquisition biasAcquisition) {
      attributes.attribute(BIAS_ACQUISITION).set(biasAcquisition);
      return this;
   }

   /**
    * Used in scattered cache. Specifies how long can be the acquired bias held; while the reads
    * will never be stale, tracking that information consumes memory on primary owner.
    */
   public ClusteringConfigurationBuilder biasLifespan(long l, TimeUnit unit) {
      attributes.attribute(BIAS_LIFESPAN).set(unit.toMillis(l));
      return this;
   }

   /**
    * Configure hash sub element
    */
   @Override
   public HashConfigurationBuilder hash() {
      return hashConfigurationBuilder;
   }

   /**
    * This method allows configuration of the L1 cache for distributed
    * caches. L1 should be explicitly enabled by calling {@link L1ConfigurationBuilder#enable()}
    */
   @Override
   public L1ConfigurationBuilder l1() {
      return l1ConfigurationBuilder;
   }

   /**
    * Configure the {@code stateTransfer} sub element for distributed and replicated caches.
    * It doesn't have any effect on LOCAL or INVALIDATION-mode caches.
    */
   @Override
   public StateTransferConfigurationBuilder stateTransfer() {
      return stateTransferConfigurationBuilder;
   }

   @Override
   public PartitionHandlingConfigurationBuilder partitionHandling() {
      return partitionHandlingConfigurationBuilder;
   }

   @Override
   public
   void validate() {
      for (Builder<?> validatable : Arrays.asList(hashConfigurationBuilder, l1ConfigurationBuilder,
            stateTransferConfigurationBuilder, partitionHandlingConfigurationBuilder)) {
         validatable.validate();
      }
      if (cacheMode().isScattered()) {
         if (hash().numOwners() != 1 && hash().isNumOwnersSet()) {
            throw log.scatteredCacheNeedsSingleOwner();
         }
         hash().numOwners(1);
         org.infinispan.transaction.TransactionMode transactionMode = transaction().transactionMode();
         if (transactionMode != null && transactionMode.isTransactional()) {
            throw log.scatteredCacheIsNonTransactional();
         }
      }
      if (!cacheMode().isScattered() && attributes.attribute(INVALIDATION_BATCH_SIZE).isModified()) {
         throw log.invalidationBatchSizeAppliesOnNonScattered();
      }
      if (!cacheMode().isScattered() && (attributes.attribute(BIAS_ACQUISITION).isModified() || attributes.attribute(BIAS_LIFESPAN).isModified())) {
         throw log.biasedReadsAppliesOnlyToScattered();
      } else if (attributes.attribute(BIAS_ACQUISITION).get() == BiasAcquisition.ON_READ) {
         throw new UnsupportedOperationException("Not implemented yet");
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      if (cacheMode().isClustered() && globalConfig.transport().transport() == null) {
         throw log.missingTransportConfiguration();
      }

      for (ConfigurationChildBuilder validatable : Arrays
            .asList(hashConfigurationBuilder, l1ConfigurationBuilder, stateTransferConfigurationBuilder, partitionHandlingConfigurationBuilder)) {
         validatable.validate(globalConfig);
      }
   }

   @Override
   public
   ClusteringConfiguration create() {
      return new ClusteringConfiguration(attributes.protect(), hashConfigurationBuilder.create(),
            l1ConfigurationBuilder.create(), stateTransferConfigurationBuilder.create(), partitionHandlingConfigurationBuilder.create());
   }

   @Override
   public ClusteringConfigurationBuilder read(ClusteringConfiguration template) {
      attributes.read(template.attributes());
      hashConfigurationBuilder.read(template.hash());
      l1ConfigurationBuilder.read(template.l1());
      stateTransferConfigurationBuilder.read(template.stateTransfer());
      partitionHandlingConfigurationBuilder.read(template.partitionHandling());

      return this;
   }

   @Override
   public String toString() {
      return "ClusteringConfigurationBuilder [hashConfigurationBuilder=" + hashConfigurationBuilder +
            ", l1ConfigurationBuilder=" + l1ConfigurationBuilder +
            ", stateTransferConfigurationBuilder=" + stateTransferConfigurationBuilder +
            ", partitionHandlingConfigurationBuilder=" + partitionHandlingConfigurationBuilder +
            ", attributes=" + attributes + "]";
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

}
