package org.infinispan.persistence.remote.configuration;

import static org.infinispan.configuration.cache.AbstractStoreConfiguration.PROPERTIES;
import static org.infinispan.configuration.cache.AbstractStoreConfiguration.SEGMENTED;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.BALANCING_STRATEGY;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.CONNECTION_TIMEOUT;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.FORCE_RETURN_VALUES;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.MARSHALLER;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.PROTOCOL_VERSION;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.REMOTE_CACHE_CONTAINER;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.REMOTE_CACHE_NAME;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.SOCKET_TIMEOUT;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.TCP_NO_DELAY;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.URI;
import static org.infinispan.persistence.remote.logging.Log.CONFIG;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.persistence.remote.configuration.global.RemoteContainersConfiguration;
import org.infinispan.persistence.remote.logging.Log;

/**
 * RemoteStoreConfigurationBuilder. Configures a
 * {@link org.infinispan.persistence.remote.RemoteStore}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class RemoteStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<RemoteStoreConfiguration, RemoteStoreConfigurationBuilder> implements
      RemoteStoreConfigurationChildBuilder<RemoteStoreConfigurationBuilder> {
   private final ExecutorFactoryConfigurationBuilder asyncExecutorFactory;
   private final ConnectionPoolConfigurationBuilder connectionPool;
   private final SecurityConfigurationBuilder security;
   private final List<RemoteServerConfigurationBuilder> servers = new ArrayList<>();

   public RemoteStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, RemoteStoreConfiguration.attributeDefinitionSet());
      asyncExecutorFactory = new ExecutorFactoryConfigurationBuilder(this);
      connectionPool = new ConnectionPoolConfigurationBuilder(this);
      security = new SecurityConfigurationBuilder(this);
   }

   @Override
   public RemoteStoreConfigurationBuilder self() {
      return this;
   }

   public ExecutorFactoryConfigurationBuilder asyncExecutorFactory() {
      return asyncExecutorFactory;
   }

   @Override
   public RemoteStoreConfigurationBuilder balancingStrategy(String balancingStrategy) {
      attributes.attribute(BALANCING_STRATEGY).set(balancingStrategy);
      return this;
   }

   @Override
   public ConnectionPoolConfigurationBuilder connectionPool() {
      return connectionPool;
   }

   @Override
   public RemoteStoreConfigurationBuilder connectionTimeout(long connectionTimeout) {
      attributes.attribute(CONNECTION_TIMEOUT).set(connectionTimeout);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder forceReturnValues(boolean forceReturnValues) {
      attributes.attribute(FORCE_RETURN_VALUES).set(forceReturnValues);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder marshaller(String marshaller) {
      attributes.attribute(MARSHALLER).set(marshaller);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder marshaller(Class<? extends Marshaller> marshaller) {
      marshaller(marshaller.getName());
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder protocolVersion(ProtocolVersion protocolVersion) {
      attributes.attribute(PROTOCOL_VERSION).set(protocolVersion);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder remoteCacheContainer(String name) {
      attributes.attribute(REMOTE_CACHE_CONTAINER).set(name);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder remoteCacheName(String remoteCacheName) {
      attributes.attribute(REMOTE_CACHE_NAME).set(remoteCacheName);
      return this;
   }

   public RemoteStoreConfigurationBuilder uri(String uri) {
      attributes.attribute(URI).set(uri);
      return this;
   }

   @Override
   public SecurityConfigurationBuilder remoteSecurity() {
      return this.security;
   }

   @Override
   public RemoteStoreConfigurationBuilder socketTimeout(long socketTimeout) {
      attributes.attribute(SOCKET_TIMEOUT).set(socketTimeout);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder tcpNoDelay(boolean tcpNoDelay) {
      attributes.attribute(TCP_NO_DELAY).set(tcpNoDelay);
      return this;
   }

   @Override
   public RemoteServerConfigurationBuilder addServer() {
      RemoteServerConfigurationBuilder builder = new RemoteServerConfigurationBuilder(this);
      this.servers.add(builder);
      return builder;
   }

   @Override
   public RemoteStoreConfiguration create() {
      List<RemoteServerConfiguration> remoteServers = new ArrayList<RemoteServerConfiguration>();
      for (RemoteServerConfigurationBuilder server : servers) {
         remoteServers.add(server.create());
      }
      return new RemoteStoreConfiguration(attributes.protect(), async.create(),
            asyncExecutorFactory.create(), connectionPool.create(), security.create(), remoteServers);
   }

   @Override
   public RemoteStoreConfigurationBuilder read(RemoteStoreConfiguration template, Combine combine) {
      super.read(template, combine);
      this.asyncExecutorFactory.read(template.asyncExecutorFactory(), combine);
      this.connectionPool.read(template.connectionPool(), combine);
      for (RemoteServerConfiguration server : template.servers()) {
         this.addServer().host(server.host()).port(server.port());
      }
      this.security.read(template.security(), combine);

      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder withProperties(Properties props) {
      String version = (String) props.remove(RemoteStoreConfiguration.PROTOCOL_VERSION.name());
      if (version != null) {
         this.protocolVersion(ProtocolVersion.parseVersion(version));
      }

      return super.withProperties(props);
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      // if no servers/uri has been specified, then there must be a remote cache container
      if ((attributes.attribute(URI).isNull() && servers.isEmpty()) && !uriPropertyDefined()) {
         String containerName = attributes.attribute(REMOTE_CACHE_CONTAINER).get();
         RemoteContainersConfiguration remoteContainersConfig = globalConfig.module(RemoteContainersConfiguration.class);
         if (remoteContainersConfig == null || !remoteContainersConfig.configurations().containsKey(containerName))
            throw Log.CONFIG.remoteStoreWithoutContainer();
      }
      super.validate(globalConfig);
   }

   private boolean uriPropertyDefined() {
      return TypedProperties.toTypedProperties(attributes.attribute(PROPERTIES).get()).containsKey(ConfigurationProperties.URI);
   }

   @Override
   public void validate() {
      this.connectionPool.validate();
      this.asyncExecutorFactory.validate();
      for (RemoteServerConfigurationBuilder server : servers) {
         server.validate();
      }
      if (attributes.attribute(SEGMENTED).get() && builder.clustering().hash().groups().isEnabled()) {
         throw CONFIG.segmentationNotSupportedWithGroups();
      }
   }
}
