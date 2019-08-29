package org.infinispan.server.hotrod.configuration;

import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.HOST;
import static org.infinispan.server.hotrod.configuration.HotRodServerConfiguration.PROXY_HOST;
import static org.infinispan.server.hotrod.configuration.HotRodServerConfiguration.PROXY_PORT;

import java.lang.invoke.MethodHandles;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.cache.LockingConfigurationBuilder;
import org.infinispan.configuration.cache.StateTransferConfigurationBuilder;
import org.infinispan.server.core.configuration.EncryptionConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * HotRodServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class HotRodServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<HotRodServerConfiguration, HotRodServerConfigurationBuilder> implements
      Builder<HotRodServerConfiguration>, HotRodServerChildConfigurationBuilder {
   private static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   private final AuthenticationConfigurationBuilder authentication = new AuthenticationConfigurationBuilder(this);
   private final TopologyCacheConfigurationBuilder topologyCache = new TopologyCacheConfigurationBuilder();
   private final EncryptionConfigurationBuilder encryption = new EncryptionConfigurationBuilder(ssl());

   public HotRodServerConfigurationBuilder() {
      super(HotRodServer.DEFAULT_HOTROD_PORT, HotRodServerConfiguration.attributeDefinitionSet());
   }

   @Override
   public HotRodServerConfigurationBuilder self() {
      return this;
   }

   @Override
   public AuthenticationConfigurationBuilder authentication() {
      return authentication;
   }

   public EncryptionConfigurationBuilder encryption() {
      return encryption;
   }

   /**
    * Sets the external address of this node, i.e. the address which clients will connect to
    */
   @Override
   public HotRodServerConfigurationBuilder proxyHost(String proxyHost) {
      attributes.attribute(PROXY_HOST).set(proxyHost);
      return this;
   }

   /**
    * Sets the external port of this node, i.e. the port which clients will connect to
    */
   @Override
   public HotRodServerConfigurationBuilder proxyPort(int proxyPort) {
      attributes.attribute(PROXY_PORT).set(proxyPort);
      return this;
   }

   /**
    * Configures the lock acquisition timeout for the topology cache. See {@link LockingConfigurationBuilder#lockAcquisitionTimeout(long)}.
    * Defaults to 10 seconds
    */
   @Override
   public HotRodServerConfigurationBuilder topologyLockTimeout(long topologyLockTimeout) {
      topologyCache.lockTimeout(topologyLockTimeout);
      return this;
   }

   /**
    * Configures the replication timeout for the topology cache. See {@link org.infinispan.configuration.cache.ClusteringConfigurationBuilder#remoteTimeout(long)}.
    * Defaults to 10 seconds
    */
   @Override
   public HotRodServerConfigurationBuilder topologyReplTimeout(long topologyReplTimeout) {
      topologyCache.replicationTimeout(topologyReplTimeout);
      return this;
   }

   /**
    * Configures whether to enable waiting for initial state transfer for the topology cache. See {@link
    * StateTransferConfigurationBuilder#awaitInitialTransfer(boolean)}
    */
   @Override
   public HotRodServerConfigurationBuilder topologyAwaitInitialTransfer(boolean topologyAwaitInitialTransfer) {
      topologyCache.awaitInitialTransfer(topologyAwaitInitialTransfer);
      return this;
   }

   /**
    * Configures whether to enable state transfer for the topology cache. If disabled, a {@link
    * org.infinispan.persistence.cluster.ClusterLoader} will be used to lazily retrieve topology information from the
    * other nodes. Defaults to true.
    */
   @Override
   public HotRodServerConfigurationBuilder topologyStateTransfer(boolean topologyStateTransfer) {
      topologyCache.lazyRetrieval(!topologyStateTransfer);
      return this;
   }

   @Override
   public HotRodServerConfiguration create() {
      return new HotRodServerConfiguration(attributes.protect(), topologyCache.create(), ssl.create(), authentication.create(), encryption.create());
   }

   @Override
   public HotRodServerConfigurationBuilder read(HotRodServerConfiguration template) {
      super.read(template);
      this.authentication.read(template.authentication());
      this.topologyCache.read(template.topologyCache());
      this.encryption.read(template.encryption());
      return this;
   }

   @Override
   public void validate() {
      super.validate();
      if (attributes.attribute(PROXY_HOST).isNull() && attributes.attribute(HOST).isNull()) {
         throw log.missingHostAddress();
      }
      authentication.validate();
      topologyCache.validate();
   }

   public HotRodServerConfiguration build(boolean validate) {
      if (validate) {
         validate();
      }
      return create();
   }

   @Override
   public HotRodServerConfiguration build() {
      return build(true);
   }

}
