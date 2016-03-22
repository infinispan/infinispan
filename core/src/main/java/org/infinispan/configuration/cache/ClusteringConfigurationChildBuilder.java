package org.infinispan.configuration.cache;

public interface ClusteringConfigurationChildBuilder extends ConfigurationChildBuilder {

   /**
    * Allows fine-tuning of rehashing characteristics. Must only used with 'distributed' cache mode.
    */
   HashConfigurationBuilder hash();

   /**
    * Configures the L1 cache behavior in 'distributed' caches instances. In any other cache modes,
    * this element is ignored.
    */
   L1ConfigurationBuilder l1();

   /**
    * Configures how state is transferred when a new cache joins the cluster.
    * Used with distribution and replication clustered modes.
    */
   StateTransferConfigurationBuilder stateTransfer();

   /**
    * If configured all communications are synchronous, in that whenever a thread sends a message
    * sent over the wire, it blocks until it receives an acknowledgment from the recipient.
    * SyncConfig is mutually exclusive with the AsyncConfig.
    *
    * @deprecated Since 9.0, the {@code replTimeout} attribute is now in {@link ClusteringConfigurationBuilder}.
    */
   @Deprecated
   SyncConfigurationBuilder sync();

   /**
    * Configures how the cache will react to cluster partitions.
    */
   PartitionHandlingConfigurationBuilder partitionHandling();
}
