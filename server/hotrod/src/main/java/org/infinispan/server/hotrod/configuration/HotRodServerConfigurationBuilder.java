package org.infinispan.server.hotrod.configuration;

import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.HOST;
import static org.infinispan.server.core.configuration.ProtocolServerConfiguration.NAME;
import static org.infinispan.server.hotrod.configuration.HotRodServerConfiguration.PROXY_HOST;
import static org.infinispan.server.hotrod.configuration.HotRodServerConfiguration.PROXY_PORT;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.LockingConfigurationBuilder;
import org.infinispan.configuration.cache.StateTransferConfigurationBuilder;
import org.infinispan.server.core.configuration.EncryptionConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.core.configuration.SaslAuthenticationConfiguration;
import org.infinispan.server.core.configuration.SaslAuthenticationConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.logging.Log;

/**
 * HotRodServerConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class HotRodServerConfigurationBuilder extends ProtocolServerConfigurationBuilder<HotRodServerConfiguration, HotRodServerConfigurationBuilder, SaslAuthenticationConfiguration> implements
      Builder<HotRodServerConfiguration>, HotRodServerChildConfigurationBuilder {

   private final SaslAuthenticationConfigurationBuilder authentication = new SaslAuthenticationConfigurationBuilder(this);
   private final TopologyCacheConfigurationBuilder topologyCache = new TopologyCacheConfigurationBuilder();
   private final EncryptionConfigurationBuilder encryption = new EncryptionConfigurationBuilder(ssl());
   private static final String DEFAULT_NAME = "hotrod";

   public HotRodServerConfigurationBuilder() {
      super(HotRodServer.DEFAULT_HOTROD_PORT, HotRodServerConfiguration.attributeDefinitionSet());
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public HotRodServerConfigurationBuilder self() {
      return this;
   }

   @Override
   public SaslAuthenticationConfigurationBuilder authentication() {
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

   @Override
   public HotRodServerConfigurationBuilder topologyLockTimeout(String topologyLockTimeout) {
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

   @Override
   public HotRodServerConfigurationBuilder topologyReplTimeout(String topologyReplTimeout) {
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

   @Override
   public HotRodServerConfigurationBuilder topologyNetworkPrefixOverride(boolean topologyNetworkPrefixOverride) {
      topologyCache.networkPrefixOverride(topologyNetworkPrefixOverride);
      return this;
   }

   @Override
   public HotRodServerConfiguration create() {
      if (!attributes.attribute(NAME).isModified()) {
         String socketBinding = socketBinding();
         name(DEFAULT_NAME + (socketBinding == null ? "" : "-" + socketBinding));
      }
      return new HotRodServerConfiguration(attributes.protect(), topologyCache.create(), ssl.create(), authentication.create(), encryption.create(), ipFilter.create());
   }

   @Override
   public HotRodServerConfigurationBuilder read(HotRodServerConfiguration template, Combine combine) {
      super.read(template, combine);
      this.topologyCache.read(template.topologyCache(), combine);
      this.encryption.read(template.encryption(), combine);
      return this;
   }

   @Override
   public void validate() {
      super.validate();
      if (attributes.attribute(PROXY_HOST).isNull() && attributes.attribute(HOST).isNull()) {
         throw Log.CONFIG.missingHostAddress();
      }
      topologyCache.validate();
      encryption.validate();
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
