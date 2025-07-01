package org.infinispan.server.hotrod.configuration;

import org.infinispan.configuration.cache.LockingConfigurationBuilder;
import org.infinispan.configuration.cache.StateTransferConfigurationBuilder;
import org.infinispan.server.core.configuration.SaslAuthenticationConfigurationBuilder;

/**
 * HotRodServerChildConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public interface HotRodServerChildConfigurationBuilder {

   /**
    * Configures authentication for this endpoint
    */
   SaslAuthenticationConfigurationBuilder authentication();

   /**
    * Sets the external address of this node, i.e. the address which clients will connect to
    */
   HotRodServerChildConfigurationBuilder proxyHost(String proxyHost);

   /**
    * Sets the external port of this node, i.e. the port which clients will connect to
    */
   HotRodServerChildConfigurationBuilder proxyPort(int proxyPort);

   /**
    * Configures the lock acquisition timeout for the topology cache. See {@link LockingConfigurationBuilder#lockAcquisitionTimeout(long)}.
    * Defaults to 10 seconds
    */
   HotRodServerChildConfigurationBuilder topologyLockTimeout(long topologyLockTimeout);

   HotRodServerConfigurationBuilder topologyLockTimeout(String topologyLockTimeout);

   /**
    * Configures the replication timeout for the topology cache. See {@link org.infinispan.configuration.cache.ClusteringConfigurationBuilder#remoteTimeout(long)}.
    * Defaults to 10 seconds
    */
   HotRodServerChildConfigurationBuilder topologyReplTimeout(long topologyReplTimeout);

   HotRodServerConfigurationBuilder topologyReplTimeout(String topologyReplTimeout);

   /**
    * Configures whether to enable waiting for initial state transfer for the topology cache. See {@link
    * StateTransferConfigurationBuilder#awaitInitialTransfer(boolean)}
    */
   HotRodServerChildConfigurationBuilder topologyAwaitInitialTransfer(boolean topologyAwaitInitialTransfer);

   /**
    * Configures whether to honor or override the network prefix returned for the available interfaces.
    * Defaults to override and to use the IANA private address conventions defined in RFC 1918
    */
   HotRodServerConfigurationBuilder topologyNetworkPrefixOverride(boolean topologyNetworkPrefixOverride);

}
