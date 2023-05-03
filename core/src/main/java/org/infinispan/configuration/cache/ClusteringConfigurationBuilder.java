package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.ClusteringConfiguration.CACHE_MODE;
import static org.infinispan.configuration.cache.ClusteringConfiguration.REMOTE_TIMEOUT;
import static org.infinispan.util.logging.Log.CONFIG;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.partitionhandling.PartitionHandling;

/**
 * Defines clustered characteristics of the cache.
 *
 * @author pmuir
 *
 */
public class ClusteringConfigurationBuilder extends AbstractConfigurationChildBuilder implements
      ClusteringConfigurationChildBuilder, Builder<ClusteringConfiguration> {
   private final HashConfigurationBuilder hashConfigurationBuilder;
   private final L1ConfigurationBuilder l1ConfigurationBuilder;
   private final StateTransferConfigurationBuilder stateTransferConfigurationBuilder;
   private final PartitionHandlingConfigurationBuilder partitionHandlingConfigurationBuilder;
   final AttributeSet attributes;

   ClusteringConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.attributes = ClusteringConfiguration.attributeDefinitionSet();
      this.hashConfigurationBuilder = new HashConfigurationBuilder(this);
      this.l1ConfigurationBuilder = new L1ConfigurationBuilder(this);
      this.stateTransferConfigurationBuilder = new StateTransferConfigurationBuilder(this);
      this.partitionHandlingConfigurationBuilder = new PartitionHandlingConfigurationBuilder(this);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
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
   public void validate() {
      for (Builder<?> validatable : Arrays.asList(hashConfigurationBuilder, l1ConfigurationBuilder,
            stateTransferConfigurationBuilder, partitionHandlingConfigurationBuilder)) {
         validatable.validate();
      }

      if (hash().numOwners() == 1 && partitionHandling().whenSplit() != PartitionHandling.ALLOW_READ_WRITES)
         throw CONFIG.singleOwnerNotSetToAllowReadWrites();
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      if (cacheMode().isClustered() && globalConfig.transport().transport() == null && !builder.template()) {
         throw CONFIG.missingTransportConfiguration();
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
}
