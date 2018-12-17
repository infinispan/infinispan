package org.infinispan.persistence.remote.configuration;

import static org.infinispan.configuration.cache.AbstractStoreConfiguration.SEGMENTED;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.BALANCING_STRATEGY;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.CONNECTION_TIMEOUT;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.FORCE_RETURN_VALUES;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.HOTROD_WRAPPING;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.KEY_SIZE_ESTIMATE;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.MARSHALLER;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.PROTOCOL_VERSION;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.RAW_VALUES;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.REMOTE_CACHE_NAME;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.SERVERS;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.SOCKET_TIMEOUT;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.TCP_NO_DELAY;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.TRANSPORT_FACTORY;
import static org.infinispan.persistence.remote.configuration.RemoteStoreConfiguration.VALUE_SIZE_ESTIMATE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.remote.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * RemoteStoreConfigurationBuilder. Configures a
 * {@link org.infinispan.persistence.remote.RemoteStore}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class RemoteStoreConfigurationBuilder extends AbstractStoreConfigurationBuilder<RemoteStoreConfiguration, RemoteStoreConfigurationBuilder> implements
      RemoteStoreConfigurationChildBuilder<RemoteStoreConfigurationBuilder>, ConfigurationBuilderInfo {
   private static final Log log = LogFactory.getLog(RemoteStoreConfigurationBuilder.class, Log.class);
   private final ExecutorFactoryConfigurationBuilder asyncExecutorFactory;
   private final ConnectionPoolConfigurationBuilder connectionPool;
   private final SecurityConfigurationBuilder security;
   private List<RemoteServerConfigurationBuilder> servers = new ArrayList<RemoteServerConfigurationBuilder>();
   private final List<ConfigurationBuilderInfo> subElements;

   public RemoteStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
      super(builder, RemoteStoreConfiguration.attributeDefinitionSet());
      asyncExecutorFactory = new ExecutorFactoryConfigurationBuilder(this);
      connectionPool = new ConnectionPoolConfigurationBuilder(this);
      security = new SecurityConfigurationBuilder(this);
      subElements = new ArrayList<>(super.getChildrenInfo());
      subElements.addAll(Arrays.asList(connectionPool, asyncExecutorFactory, security));
   }

   @Override
   public RemoteStoreConfigurationBuilder self() {
      return this;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return RemoteStoreConfiguration.ELELEMENT_DEFINITION;
   }

   @Override
   public Collection<ConfigurationBuilderInfo> getChildrenInfo() {
      return subElements;
   }

   @Override
   public ConfigurationBuilderInfo getNewBuilderInfo(String name) {
      if (name.equals(Element.SERVERS.getLocalName())) return addServer();
      return this;
   }

   @Override
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
   public RemoteStoreConfigurationBuilder hotRodWrapping(boolean hotRodWrapping) {
      attributes.attribute(HOTROD_WRAPPING).set(hotRodWrapping);
      this.rawValues(true);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder keySizeEstimate(int keySizeEstimate) {
      attributes.attribute(KEY_SIZE_ESTIMATE).set(keySizeEstimate);
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

   @Deprecated
   @Override
   public RemoteStoreConfigurationBuilder protocolVersion(String protocolVersion) {
      attributes.attribute(PROTOCOL_VERSION).set(ProtocolVersion.parseVersion(protocolVersion));
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder protocolVersion(ProtocolVersion protocolVersion) {
      attributes.attribute(PROTOCOL_VERSION).set(protocolVersion);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder rawValues(boolean rawValues) {
      attributes.attribute(RAW_VALUES).set(rawValues);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder remoteCacheName(String remoteCacheName) {
      attributes.attribute(REMOTE_CACHE_NAME).set(remoteCacheName);
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
   public RemoteStoreConfigurationBuilder transportFactory(String transportFactory) {
      attributes.attribute(TRANSPORT_FACTORY).set(transportFactory);
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder transportFactory(Class<? extends ChannelFactory> transportFactory) {
      transportFactory(transportFactory.getName());
      return this;
   }

   @Override
   public RemoteStoreConfigurationBuilder valueSizeEstimate(int valueSizeEstimate) {
      attributes.attribute(VALUE_SIZE_ESTIMATE).set(valueSizeEstimate);
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
      attributes.attribute(SERVERS).set(remoteServers);
      return new RemoteStoreConfiguration(attributes.protect(), async.create(), singletonStore.create(),
            asyncExecutorFactory.create(), connectionPool.create(), security.create());
   }

   @Override
   public RemoteStoreConfigurationBuilder read(RemoteStoreConfiguration template) {
      super.read(template);
      this.asyncExecutorFactory.read(template.asyncExecutorFactory());
      this.connectionPool.read(template.connectionPool());
      for (RemoteServerConfiguration server : template.servers()) {
         this.addServer().host(server.host()).port(server.port());
      }
      this.security.read(template.security());

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
   public void validate() {
      this.connectionPool.validate();
      this.asyncExecutorFactory.validate();
      for (RemoteServerConfigurationBuilder server : servers) {
         server.validate();
      }

      if (attributes.attribute(HOTROD_WRAPPING).get() && attributes.attribute(MARSHALLER).get() != null) {
         throw log.cannotEnableHotRodWrapping();
      }

      ProtocolVersion version = attributes.attribute(PROTOCOL_VERSION).get();
      ProtocolVersion minimumVersion = ProtocolVersion.PROTOCOL_VERSION_23;
      if (attributes.attribute(SEGMENTED).get() && version.compareTo(minimumVersion) < 0) {
         throw log.segmentationNotSupportedInThisVersion(minimumVersion);
      }
   }
}
