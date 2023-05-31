package org.infinispan.anchored.configuration;

import static org.infinispan.anchored.impl.Log.CONFIG;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.Experimental;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PartitionHandlingConfiguration;
import org.infinispan.configuration.cache.StateTransferConfiguration;
import org.infinispan.conflict.EntryMergePolicy;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.partitionhandling.PartitionHandling;

/**
 * Configuration module builder to transform an {@link CacheMode#INVALIDATION_SYNC} cache into an "anchored keys" cache.
 *
 * <p>Usage:
 * <pre>
 * ConfigurationBuilder cacheBuilder = new ConfigurationBuilder();
 * cacheBuilder.clustering().cacheMode(CacheMode.INVALIDATION_SYNC);
 * cacheBuilder.addModule(AnchoredKeysConfigurationBuilder.class).enabled(true);
 * </pre>
 * </p>
 * @see AnchoredKeysConfiguration
 *
 * @since 11
 * @author Dan Berindei
 */
@Experimental
public class AnchoredKeysConfigurationBuilder implements Builder<AnchoredKeysConfiguration> {
   private final AttributeSet attributes;
   private final ConfigurationBuilder rootBuilder;

   public AnchoredKeysConfigurationBuilder(ConfigurationBuilder builder) {
      rootBuilder = builder;
      this.attributes = AnchoredKeysConfiguration.attributeSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * Enable or disable anchored keys.
    */
   public void enabled(boolean enabled) {
      attributes.attribute(AnchoredKeysConfiguration.ENABLED).set(enabled);
   }

   @Override
   public void validate() {
      if (!rootBuilder.clustering().cacheMode().isReplicated()) {
         throw CONFIG.replicationModeRequired();
      }
      if (rootBuilder.transaction().transactionMode() != null &&
          rootBuilder.transaction().transactionMode().isTransactional()) {
         throw CONFIG.transactionsNotSupported();
      }
      Attribute<Boolean> stateTransferEnabledAttribute =
            rootBuilder.clustering().stateTransfer().attributes()
                       .attribute(StateTransferConfiguration.FETCH_IN_MEMORY_STATE);
      if (!stateTransferEnabledAttribute.get()) {
         throw CONFIG.stateTransferRequired();
      }

      Attribute<Boolean> awaitStateTransferAttribute =
            rootBuilder.clustering().stateTransfer().attributes()
                       .attribute(StateTransferConfiguration.AWAIT_INITIAL_TRANSFER);
      if (awaitStateTransferAttribute.get() && awaitStateTransferAttribute.isModified()) {
         throw CONFIG.awaitInitialTransferNotSupported();
      }
      rootBuilder.clustering().stateTransfer().awaitInitialTransfer(false);

      Attribute<PartitionHandling> whenSplitAttribute =
            rootBuilder.clustering().partitionHandling().attributes()
                       .attribute(PartitionHandlingConfiguration.WHEN_SPLIT);
      if (whenSplitAttribute.get() != PartitionHandling.ALLOW_READ_WRITES && whenSplitAttribute.isModified()) {
         throw CONFIG.whenSplitNotSupported();
      }
      rootBuilder.clustering().partitionHandling().whenSplit(PartitionHandling.ALLOW_READ_WRITES);

      Attribute<EntryMergePolicy> mergePolicyAttribute =
            rootBuilder.clustering().partitionHandling().attributes()
                       .attribute(PartitionHandlingConfiguration.MERGE_POLICY);
      if (mergePolicyAttribute.get() != MergePolicy.PREFERRED_NON_NULL && mergePolicyAttribute.isModified()) {
         throw CONFIG.mergePolicyNotSupported();
      }
      rootBuilder.clustering().partitionHandling().mergePolicy(MergePolicy.PREFERRED_NON_NULL);
   }

   @Override
   public AnchoredKeysConfiguration create() {
      return new AnchoredKeysConfiguration(attributes);
   }

   @Override
   public Builder<?> read(AnchoredKeysConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }
}
