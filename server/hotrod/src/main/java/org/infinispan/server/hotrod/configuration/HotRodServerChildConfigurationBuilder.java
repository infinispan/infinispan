package org.infinispan.server.hotrod.configuration;

import org.infinispan.configuration.cache.LockingConfigurationBuilder;
import org.infinispan.configuration.cache.StateTransferConfigurationBuilder;
import org.infinispan.configuration.cache.SyncConfigurationBuilder;

/**
 * HotRodServerChildConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public interface HotRodServerChildConfigurationBuilder {

   AuthenticationConfigurationBuilder authentication();

   /**
    * Sets the external address of this node, i.e. the address which clients will connect to
    */
   HotRodServerChildConfigurationBuilder proxyHost(String proxyHost);

   /**
    * Sets the external port of this node, i.e. the port which clients will connect to
    */
   HotRodServerChildConfigurationBuilder proxyPort(int proxyPort);

   /**
    * Configures the lock acquisition timeout for the topology cache. See {@link LockingConfigurationBuilder#lockAcquisitionTimeout(long)}. Defaults to 10 seconds
    */
   HotRodServerChildConfigurationBuilder topologyLockTimeout(long topologyLockTimeout);

   /**
    * Configures the replication timeout for the topology cache. See {@link SyncConfigurationBuilder#replTimeout(long)}. Defaults to 10 seconds
    */
   HotRodServerChildConfigurationBuilder topologyReplTimeout(long topologyReplTimeout);

   /**
    * Configures whether to enable waiting for initial state transfer for the topology cache. See {@link StateTransferConfigurationBuilder#awaitInitialTransfer(boolean)}
    */
   HotRodServerChildConfigurationBuilder topologyAwaitInitialTransfer(boolean topologyAwaitInitialTransfer);

   /**
    * Configures whether to enable state transfer for the topology cache. If disabled, a {@link org.infinispan.persistence.cluster.ClusterLoader} will be used to lazily retrieve topology information from the other nodes.
    * Defaults to true.
    */
   HotRodServerChildConfigurationBuilder topologyStateTransfer(boolean topologyStateTransfer);

}
