package org.infinispan.config;

import org.infinispan.CacheException;
import org.infinispan.Version;
import org.infinispan.executors.DefaultExecutorFactory;
import org.infinispan.executors.DefaultScheduledExecutorFactory;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.MBeanServerLookup;
import org.infinispan.jmx.PlatformMBeanServerLookup;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.Externalizer;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.Util;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Configuration component that encapsulates the global configuration.
 * <p/>
 * 
 * A default instance of this bean takes default values for each attribute.  Please see the individual setters for
 * details of what these defaults are.
 * <p/>
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 * 
 * @see <a href="../../../config.html#ce_infinispan_global">Configuration reference</a>
 * 
 */
@SurvivesRestarts
@Scope(Scopes.GLOBAL)
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
@ConfigurationDoc(name="global",desc="Defines global settings shared among all cache instances created by a single CacheManager.")
public class GlobalConfiguration extends AbstractConfigurationBean {
   
   /**
    * Configures executor factory.
    * 
    * @author Vladimir Blagojevic
    * @since 5.0
    */
   public interface ExecutorFactoryConfig<T> {
      /**
       * Specify factory class for executor
       * 
       * @param factory
       *           clazz
       * @return this ExecutorFactoryConfig
       */
      ExecutorFactoryConfig<T> factory(Class<? extends T> clazz);

      /**
       * Add key/value property pair to this executor factory configuration
       * 
       * @param key
       *           property key
       * @param value
       *           property value
       * @return previous value if exists, null otherwise
       */
      Object addProperty(String key, String value);

      /**
       * Set key/value properties to this executor factory configuration
       * 
       * @param props
       *           Prpperties
       * @return this ExecutorFactoryConfig
       */
      ExecutorFactoryConfig<T> withProperties(Properties props);
   }

   /**
    * Configures whether global statistics are gathered and reported via JMX for all caches under this cache manager.
    * 
    * @author Vladimir Blagojevic
    * @since 5.0
    */
   public interface GlobalJmxStatisticsConfig {
      /**
       * Toggle to enable/disable global statistics being exported via JMX
       * 
       * @param exposeGlobalJmxStatistics
       */
      GlobalJmxStatisticsConfig enabled(Boolean enabled);

      /**
       * Sets properties which are then passed to the MBean Server Lookup implementation specified.
       * 
       * @param properties
       *           properties to pass to the MBean Server Lookup
       */
      GlobalJmxStatisticsConfig setProperties(Properties p);

      /**
       * If JMX statistics are enabled then all 'published' JMX objects will appear under this name.
       * This is optional, if not specified an object name will be created for you by default.
       * 
       * @param jmxObjectName
       */
      GlobalJmxStatisticsConfig jmxDomain(String jmxDomain);

      /**
       * Instance of class that will attempt to locate a JMX MBean server to bind to
       * 
       * @param mBeanServerLookupClass
       *           MBean Server Lookup class implementation
       */
      GlobalJmxStatisticsConfig mBeanServerLookup(Class<? extends MBeanServerLookup> beanServerLookupClass);

      /**
       * If true, multiple cache manager instances could be configured under the same configured JMX
       * domain. Each cache manager will in practice use a different JMX domain that has been
       * calculated based on the configured one by adding an incrementing index to it.
       * 
       * @param allowDuplicateDomains
       */
      GlobalJmxStatisticsConfig allowDuplicateDomains(Boolean allowDuplicateDomains);

      /**
       * If JMX statistics are enabled, this property represents the name of this cache manager. It
       * offers the possibility for clients to provide a user-defined name to the cache manager
       * which later can be used to identify the cache manager within a JMX based management tool
       * amongst other cache managers that might be running under the same JVM.
       * 
       * @param cacheManagerName
       */
      GlobalJmxStatisticsConfig cacheManagerName(String cacheManagerName);

      /**
       * Sets the instance of the {@link MBeanServerLookup} class to be used to bound JMX MBeans to.
       * 
       * @param mBeanServerLookupInstance
       *           An instance of {@link MBeanServerLookup}
       */
      GlobalJmxStatisticsConfig usingMBeanServerLookupInstance(MBeanServerLookup MBeanServerLookupInstance);
   }

   /**
    * Configures the transport used for network communications across the cluster.
    * 
    * @author Vladimir Blagojevic
    * @since 5.0
    */
   public interface TransportConfig {
      /**
       * Defines the name of the cluster. Nodes only connect to clusters sharing the same name.
       * 
       * @param clusterName
       */
      TransportConfig clusterName(String clusterName);

      /**
       * The id of the machine where this node runs. Used for <a
       * href="http://community.jboss.org/wiki/DesigningServerHinting">server hinting</a> .
       */
      TransportConfig machineId(String machineId);

      /**
       * The id of the rack where this node runs. Used for <a
       * href="http://community.jboss.org/wiki/DesigningServerHinting">server hinting</a> .
       */
      TransportConfig rackId(String rackId);

      /**
       * The id of the site where this node runs. Used for <a
       * href="http://community.jboss.org/wiki/DesigningServerHinting">server hinting</a> .
       */
      TransportConfig siteId(String siteId);

      TransportConfig distributedSyncTimeout(Long distributedSyncTimeout);

      /**
       * Class that represents a network transport. Must implement
       * org.infinispan.remoting.transport.Transport
       * 
       * @param transportClass
       */
      TransportConfig transportClass(Class<? extends Transport> transportClass);

      /**
       * Name of the current node. This is a friendly name to make logs, etc. make more sense.
       * Defaults to a combination of host name and a random number (to differentiate multiple nodes
       * on the same host)
       * 
       * @param nodeName
       */
      TransportConfig nodeName(String nodeName);

      
      /**
       * Sets transport properties
       * 
       * @param properties
       * @return this TransportConfig
       */
      TransportConfig withProperties(Properties properties);

      /**
       * If set to true, RPC operations will fail if the named cache does not exist on remote nodes
       * with a NamedCacheNotFoundException. Otherwise, operations will succeed but it will be
       * logged on the caller that the RPC did not succeed on certain nodes due to the named cache
       * not being available.
       * 
       * @param strictPeerToPeer
       *           flag controlling this behavior
       */
      TransportConfig strictPeerToPeer(Boolean strictPeerToPeer);

      Object addProperty(String key, String value);
   }

   /**
    * Configures serialization and marshalling settings.
    * 
    * @author Vladimir Blagojevic
    * @since 5.0
    */
   public interface SerializationConfig {
      /**
       * Fully qualified name of the marshaller to use. It must implement
       * org.infinispan.marshall.StreamingMarshaller
       * 
       * @param marshallerClass
       */
      SerializationConfig marshallerClass(Class<? extends Marshaller> c);

      /**
       * Largest allowable version to use when marshalling internal state. Set this to the lowest
       * version cache instance in your cluster to ensure compatibility of communications. However,
       * setting this too low will mean you lose out on the benefit of improvements in newer
       * versions of the marshaller.
       * 
       * @param marshallVersion
       */
      SerializationConfig version(String s);

      /**
       * Returns externalizers sub element
       * 
       * @return ExternalizersConfig element
       */
      ExternalizersConfig configureExternalizers();
   }

   /**
    * ExternalizersConfig.
    * 
    * @author Vladimir Blagojevic
    * @since 5.0
    */
   public interface ExternalizersConfig {
      
      /**
       * Adds externalizer config to the list of configured externalizers 
       * @param ec 
       * @return this ExternalizersConfig
       */
      ExternalizersConfig addExtrenalizer(ExternalizerConfig ec);

      /**
       * Adds externalizer config as a class and id to the list of configured externalizers 
       * 
       * @param <T>
       * @param clazz externalizer class
       * @param id id of externlizer
       * @return this ExternalizersConfig
       */
      <T> ExternalizersConfig addExtrenalizer(Class<? extends Externalizer<T>> clazz, int id);
   }

   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = 8910865501990177720L;

   public GlobalConfiguration() {
      super();
   }

   /**
    * Default replication version, from {@link org.infinispan.Version#getVersionShort}.
    */
   public static final short DEFAULT_MARSHALL_VERSION = Version.getVersionShort();

   @XmlElement
   ExecutorFactoryType asyncListenerExecutor = new ExecutorFactoryType();

   @XmlElement
   ExecutorFactoryType asyncTransportExecutor = new ExecutorFactoryType();

   @XmlElement
   ScheduledExecutorFactoryType evictionScheduledExecutor = new ScheduledExecutorFactoryType();

   @XmlElement
   ScheduledExecutorFactoryType replicationQueueScheduledExecutor = new ScheduledExecutorFactoryType();

   @XmlElement
   GlobalJmxStatisticsType globalJmxStatistics = new GlobalJmxStatisticsType();

   @XmlElement
   TransportType transport = new TransportType(null);

   @XmlElement
   SerializationType serialization = new SerializationType();

   @XmlElement
   ShutdownType shutdown = new ShutdownType();

   @XmlTransient
   GlobalComponentRegistry gcr;
   
   
   public ExecutorFactoryConfig<ExecutorFactory> configureAsyncListenerExecutor(){
      return asyncListenerExecutor;
   }
   
   public ExecutorFactoryConfig<ExecutorFactory> configureAsyncTransportExecutor(){
      return asyncTransportExecutor;
   }
   
   public ExecutorFactoryConfig<ScheduledExecutorFactory> configureEvictionScheduledExecutor(){
      return evictionScheduledExecutor;
   }
   
   public ExecutorFactoryConfig<ScheduledExecutorFactory> configureReplicationQueueScheduledExecutor(){
      return replicationQueueScheduledExecutor;
   }
   
   public GlobalJmxStatisticsConfig configureGlobalJmxStatistics(){
      return globalJmxStatistics;
   }
   
   public SerializationConfig configureSerialization(){
      return serialization;
   }
   
   public TransportConfig configureTransport(){
      return transport;
   }

   public boolean isExposeGlobalJmxStatistics() {
      return globalJmxStatistics.enabled;
   }

   /**
    * Toggle to enable/disable global statistics being exported via JMX
    * 
    * @param exposeGlobalJmxStatistics
    */
   @Deprecated
   public void setExposeGlobalJmxStatistics(boolean exposeGlobalJmxStatistics) {
      testImmutability("exposeGlobalManagementStatistics");
      globalJmxStatistics.enabled(exposeGlobalJmxStatistics);
   }

   /**
    * If JMX statistics are enabled then all 'published' JMX objects will appear under this name.
    * This is optional, if not specified an object name will be created for you by default.
    * 
    * @param jmxObjectName
    */
   @Deprecated
   public void setJmxDomain(String jmxObjectName) {
      globalJmxStatistics.setJmxDomain(jmxObjectName);
   }

   /**
    * @see #setJmxDomain(String)
    */
   public String getJmxDomain() {
      return globalJmxStatistics.jmxDomain;
   }

   public String getMBeanServerLookup() {
      return globalJmxStatistics.mBeanServerLookup;
   }

   public Properties getMBeanServerProperties() {
      return globalJmxStatistics.properties;
   }

   /**
    * Sets properties which are then passed to the MBean Server Lookup implementation specified.
    * @param properties properties to pass to the MBean Server Lookup
    */
   @Deprecated
   public void setMBeanServerProperties(Properties properties) {
      globalJmxStatistics.setProperties(toTypedProperties(properties));
   }

   /**
    * Fully qualified name of class that will attempt to locate a JMX MBean server to bind to
    * 
    * @param mBeanServerLookupClass fully qualified class name of the MBean Server Lookup class implementation
    */
   @Deprecated
   public void setMBeanServerLookup(String mBeanServerLookupClass) {
      globalJmxStatistics.setMBeanServerLookup(mBeanServerLookupClass);
   }

   /**
    * @deprecated Use {@link #setMBeanServerLookupInstance(org.infinispan.jmx.MBeanServerLookup)} instead.
    */
   @XmlTransient
   @Deprecated
   public void setMBeanServerLookup(MBeanServerLookup mBeanServerLookup) {
      globalJmxStatistics.setMBeanServerLookupInstance(mBeanServerLookup);
   }

   /**
    * Sets the instance of the {@link MBeanServerLookup} class to be used to
    * bound JMX MBeans to.
    *
    * @param mBeanServerLookupInstance An instance of {@link MBeanServerLookup}
    */
   @XmlTransient
   @Deprecated
   public void setMBeanServerLookupInstance(MBeanServerLookup mBeanServerLookupInstance) {
      globalJmxStatistics.setMBeanServerLookupInstance(mBeanServerLookupInstance);
   }

   public MBeanServerLookup getMBeanServerLookupInstance() {
      return globalJmxStatistics.getMBeanServerLookupInstance();
   }

   public boolean isAllowDuplicateDomains() {
      return globalJmxStatistics.allowDuplicateDomains;
   }

   /**
    * If true, multiple cache manager instances could be configured under the same configured JMX
    * domain. Each cache manager will in practice use a different JMX domain that has been
    * calculated based on the configured one by adding an incrementing index to it.
    * 
    * @param allowDuplicateDomains
    */
   @Deprecated
   public void setAllowDuplicateDomains(boolean allowDuplicateDomains) {
      globalJmxStatistics.setAllowDuplicateDomains(allowDuplicateDomains);
   }

   public String getCacheManagerName() {
      return globalJmxStatistics.cacheManagerName;
   }

   /**
    * If JMX statistics are enabled, this property represents the name of this cache manager.
    * It offers the possibility for clients to provide a user-defined name to the cache manager
    * which later can be used to identify the cache manager within a JMX based management tool
    * amongst other cache managers that might be running under the same JVM.
    *
    * @param cacheManagerName
    */
   @Deprecated
   public void setCacheManagerName(String cacheManagerName) {
      globalJmxStatistics.setCacheManagerName(cacheManagerName);
   }

   public boolean isStrictPeerToPeer() {
      return transport.strictPeerToPeer;
   }

   /**
    * If set to true, RPC operations will fail if the named cache does not exist on remote nodes
    * with a NamedCacheNotFoundException.  Otherwise, operations will succeed but it will be
    * logged on the caller that the RPC did not succeed on certain nodes due to the named cache
    * not being available.
    * 
    * @param strictPeerToPeer flag controlling this behavior
    */
   @Deprecated
   public void setStrictPeerToPeer(boolean strictPeerToPeer) {
      transport.setStrictPeerToPeer(strictPeerToPeer);
   }

   public boolean hasTopologyInfo() {
      return getSiteId() != null || getRackId() != null || getMachineId() != null;
   }

   /**
    * Behavior of the JVM shutdown hook registered by the cache
    */
   public static enum ShutdownHookBehavior {
      /**
       * By default a shutdown hook is registered if no MBean server (apart from the JDK default) is detected.
       */
      DEFAULT,
      /**
       * Forces the cache to register a shutdown hook even if an MBean server is detected.
       */
      REGISTER,
      /**
       * Forces the cache NOT to register a shutdown hook, even if no MBean server is detected.
       */
      DONT_REGISTER
   }

   @Inject
   private void injectDependencies(GlobalComponentRegistry gcr) {
      this.gcr = gcr;
      gcr.registerComponent(asyncListenerExecutor, "asyncListenerExecutor");
      gcr.registerComponent(asyncTransportExecutor, "asyncTransportExecutor");
      gcr.registerComponent(evictionScheduledExecutor, "evictionScheduledExecutor");
      gcr.registerComponent(replicationQueueScheduledExecutor, "replicationQueueScheduledExecutor");
      gcr.registerComponent(replicationQueueScheduledExecutor, "replicationQueueScheduledExecutor");
      gcr.registerComponent(globalJmxStatistics, "globalJmxStatistics");
      gcr.registerComponent(transport, "transport");
      gcr.registerComponent(serialization, "serialization");
      gcr.registerComponent(shutdown, "shutdown");
   }

   protected boolean hasComponentStarted() {
      return gcr != null && gcr.getStatus() != null && gcr.getStatus() == ComponentStatus.RUNNING;
   }

   public String getAsyncListenerExecutorFactoryClass() {
      return asyncListenerExecutor.factory;
   }

   @Deprecated
   public void setAsyncListenerExecutorFactoryClass(String asyncListenerExecutorFactoryClass) {
      asyncListenerExecutor.setFactory(asyncListenerExecutorFactoryClass);
   }

   public String getAsyncTransportExecutorFactoryClass() {
      return asyncTransportExecutor.factory;
   }

   @Deprecated
   public void setAsyncTransportExecutorFactoryClass(String asyncTransportExecutorFactoryClass) {
      this.asyncTransportExecutor.setFactory(asyncTransportExecutorFactoryClass);
   }

   public String getEvictionScheduledExecutorFactoryClass() {
      return evictionScheduledExecutor.factory;
   }

   @Deprecated
   public void setEvictionScheduledExecutorFactoryClass(String evictionScheduledExecutorFactoryClass) {
      evictionScheduledExecutor.setFactory(evictionScheduledExecutorFactoryClass);
   }

   public String getReplicationQueueScheduledExecutorFactoryClass() {
      return replicationQueueScheduledExecutor.factory;
   }


   @Deprecated
   public void setReplicationQueueScheduledExecutorFactoryClass(String replicationQueueScheduledExecutorFactoryClass) {
      replicationQueueScheduledExecutor.setFactory(replicationQueueScheduledExecutorFactoryClass);
   }

   public String getMarshallerClass() {
      return serialization.marshallerClass;
   }

   /**
    * Fully qualified name of the marshaller to use. It must implement
    * org.infinispan.marshall.StreamingMarshaller
    * 
    * @param marshallerClass
    */
   @Deprecated
   public void setMarshallerClass(String marshallerClass) {
      serialization.setMarshallerClass(marshallerClass);
   }

   public String getTransportNodeName() {
      return transport.nodeName;
   }

   /**
    * Name of the current node. This is a friendly name to make logs, etc. make more sense. Defaults
    * to a combination of host name and a random number (to differentiate multiple nodes on the same
    * host)
    * 
    * @param nodeName
    */
   @Deprecated
   public void setTransportNodeName(String nodeName) {
      transport.setNodeName(nodeName);
   }

   public String getTransportClass() {
      return transport.transportClass;
   }


   /**
    * Fully qualified name of a class that represents a network transport. Must implement
    * org.infinispan.remoting.transport.Transport
    * 
    * @param transportClass
    */
   @Deprecated
   public void setTransportClass(String transportClass) {
      transport.setTransportClass(transportClass);
   }

   public Properties getTransportProperties() {
      return transport.properties;
   }

   @Deprecated
   public void setTransportProperties(Properties transportProperties) {
      transport.setProperties(toTypedProperties(transportProperties));
   }

   @Deprecated
   public void setTransportProperties(String transportPropertiesString) {
      transport.setProperties(toTypedProperties(transportPropertiesString));
   }
   
   public String getClusterName() {
      return transport.clusterName;
   }

   /**
    * Defines the name of the cluster. Nodes only connect to clusters sharing the same name.
    * 
    * @param clusterName
    */
   @Deprecated
   public void setClusterName(String clusterName) {
      transport.setClusterName(clusterName);
   }

   /**
    * The id of the machine where this node runs. Used for <a href="http://community.jboss.org/wiki/DesigningServerHinting">server
    * hinting</a> .
    */
   @Deprecated
   public void setMachineId(String machineId) {
      transport.setMachineId(machineId);
   }

   /**
    * @see #setMachineId(String)
    */
   public String getMachineId() {
      return transport.machineId;
   }

   /**
    * The id of the rack where this node runs. Used for <a href="http://community.jboss.org/wiki/DesigningServerHinting">server
    * hinting</a> .
    */
   @Deprecated
   public void setRackId(String rackId) {
      transport.setRackId(rackId);
   }

   /**
    * @see #setRackId(String)
    */
   public String getRackId() {
      return transport.rackId;
   }

   /**
    * The id of the site where this node runs. Used for <a href="http://community.jboss.org/wiki/DesigningServerHinting">server
    * hinting</a> .
    */
   @Deprecated
   public void setSiteId(String siteId) {
      transport.setSiteId(siteId);
   }

   /**
    * @see #setSiteId(String) 
    */
   public String getSiteId() {
      return transport.siteId;
   }


   public ShutdownHookBehavior getShutdownHookBehavior() {
      return shutdown.hookBehavior;
   }

   /**
    * Behavior of the JVM shutdown hook registered by the cache. The options available are: DEFAULT
    * - A shutdown hook is registered even if no MBean server (apart from the JDK default) is
    * detected. REGISTER - Forces the cache to register a shutdown hook even if an MBean server is
    * detected. DONT_REGISTER - Forces the cache NOT to register a shutdown hook, even if no MBean
    * server is detected.
    * 
    * @param shutdownHookBehavior
    */
   public void setShutdownHookBehavior(ShutdownHookBehavior shutdownHookBehavior) {
      shutdown.setHookBehavior(shutdownHookBehavior);
   }

   public void setShutdownHookBehavior(String shutdownHookBehavior) {
      if (shutdownHookBehavior == null)
         throw new ConfigurationException("Shutdown hook behavior cannot be null", "ShutdownHookBehavior");
      ShutdownHookBehavior temp = ShutdownHookBehavior.valueOf(uc(shutdownHookBehavior));
      if (temp == null) {
         log.warn("Unknown shutdown hook behavior '" + shutdownHookBehavior + "', using defaults.");
         temp = ShutdownHookBehavior.DEFAULT;
      }
      setShutdownHookBehavior(temp);
   }

   public Properties getAsyncListenerExecutorProperties() {
      return asyncListenerExecutor.properties;
   }


   @Deprecated
   public void setAsyncListenerExecutorProperties(Properties asyncListenerExecutorProperties) {
      asyncListenerExecutor.setProperties(toTypedProperties(asyncListenerExecutorProperties));
   }

   @Deprecated
   public void setAsyncListenerExecutorProperties(String asyncListenerExecutorPropertiesString) {
      asyncListenerExecutor.setProperties(toTypedProperties(asyncListenerExecutorPropertiesString));
   }

   public Properties getAsyncTransportExecutorProperties() {
      return asyncTransportExecutor.properties;
   }

   @Deprecated
   public void setAsyncTransportExecutorProperties(Properties asyncTransportExecutorProperties) {
      this.asyncTransportExecutor.setProperties(toTypedProperties(asyncTransportExecutorProperties));
   }

   @Deprecated
   public void setAsyncTransportExecutorProperties(String asyncSerializationExecutorPropertiesString) {
      this.asyncTransportExecutor.setProperties(toTypedProperties(asyncSerializationExecutorPropertiesString));
   }

   public Properties getEvictionScheduledExecutorProperties() {
      return evictionScheduledExecutor.properties;
   }

   @Deprecated
   public void setEvictionScheduledExecutorProperties(Properties evictionScheduledExecutorProperties) {
      evictionScheduledExecutor.setProperties(toTypedProperties(evictionScheduledExecutorProperties));
   }

   @Deprecated
   public void setEvictionScheduledExecutorProperties(String evictionScheduledExecutorPropertiesString) {
      evictionScheduledExecutor.setProperties(toTypedProperties(evictionScheduledExecutorPropertiesString));
   }

   public Properties getReplicationQueueScheduledExecutorProperties() {
      return replicationQueueScheduledExecutor.properties;
   }

   @Deprecated
   public void setReplicationQueueScheduledExecutorProperties(Properties replicationQueueScheduledExecutorProperties) {
      this.replicationQueueScheduledExecutor.setProperties(toTypedProperties(replicationQueueScheduledExecutorProperties));
   }

   @Deprecated
   public void setReplicationQueueScheduledExecutorProperties(String replicationQueueScheduledExecutorPropertiesString) {
      this.replicationQueueScheduledExecutor.setProperties(toTypedProperties(replicationQueueScheduledExecutorPropertiesString));
   }

   public short getMarshallVersion() {
      return Version.getVersionShort(serialization.version);
   }

   public String getMarshallVersionString() {
      return serialization.version;
   }

   /**
    * Largest allowable version to use when marshalling internal state. Set this to the lowest
    * version cache instance in your cluster to ensure compatibility of communications. However,
    * setting this too low will mean you lose out on the benefit of improvements in newer versions
    * of the marshaller.
    * 
    * @param marshallVersion
    */
   @Deprecated
   public void setMarshallVersion(short marshallVersion) {
      testImmutability("marshallVersion");
      serialization.version = Version.decodeVersionForSerialization(marshallVersion);
   }

   /**
    * Largest allowable version to use when marshalling internal state. Set this to the lowest
    * version cache instance in your cluster to ensure compatibility of communications. However,
    * setting this too low will mean you lose out on the benefit of improvements in newer versions
    * of the marshaller.
    * 
    * @param marshallVersion
    */
   @Deprecated
   public void setMarshallVersion(String marshallVersion) {
      serialization.setVersion(marshallVersion);
   }

   /**
    * Helper method that allows for quick registration of {@link Externalizer} implementations.
    *
    * @param externalizers
    */
   public void addExternalizer(Externalizer... externalizers) {
      for (Externalizer ext : externalizers) {
         serialization.externalizerTypes.getExternalizerConfigs().add(new ExternalizerConfig().setExternalizer(ext));
      }
   }

   /**
    * Helper method that allows for quick registration of an {@link Externalizer} implementation
    * alongside its corresponding identifier. Remember that the identifier needs to a be positive
    * number, including 0, and cannot clash with other identifiers in the system.
    *
    * @param id
    * @param externalizer
    */
   public void addExternalizer(int id, Externalizer externalizer) {
      ExternalizerConfig config = new ExternalizerConfig().setExternalizer(externalizer).setId(id);
      serialization.externalizerTypes.getExternalizerConfigs().add(config);
   }

   public void setExternalizersType(ExternalizersType externalizersType) {
      serialization.setExternalizerTypes(externalizersType);
   }

   public ExternalizersType getExternalizersType() {
      return serialization.externalizerTypes;
   }

   public long getDistributedSyncTimeout() {
      return transport.distributedSyncTimeout;
   }

   @Deprecated
   public void setDistributedSyncTimeout(long distributedSyncTimeout) {
      transport.distributedSyncTimeout = distributedSyncTimeout;
   }

   public void accept(ConfigurationBeanVisitor v) {
      asyncListenerExecutor.accept(v);
      asyncTransportExecutor.accept(v);
      evictionScheduledExecutor.accept(v);
      globalJmxStatistics.accept(v);
      replicationQueueScheduledExecutor.accept(v);
      serialization.accept(v);
      shutdown.accept(v);
      transport.accept(v);
      v.visitGlobalConfiguration(this);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      GlobalConfiguration that = (GlobalConfiguration) o;

      if (!serialization.version.equals(that.serialization.version)) return false;
      if (asyncListenerExecutor.factory != null ? !asyncListenerExecutor.factory.equals(that.asyncListenerExecutor.factory) : that.asyncListenerExecutor.factory != null)
         return false;
      if (asyncListenerExecutor.properties != null ? !asyncListenerExecutor.properties.equals(that.asyncListenerExecutor.properties) : that.asyncListenerExecutor.properties != null)
         return false;
      if (asyncTransportExecutor.factory != null ? !asyncTransportExecutor.factory.equals(that.asyncTransportExecutor.factory) : that.asyncTransportExecutor.factory != null)
         return false;
      if (asyncTransportExecutor.properties != null ? !asyncTransportExecutor.properties.equals(that.asyncTransportExecutor.properties) : that.asyncTransportExecutor.properties != null)
         return false;
      if (transport.clusterName != null ? !transport.clusterName.equals(that.transport.clusterName) : that.transport.clusterName != null)
         return false;
      if (evictionScheduledExecutor.factory != null ? !evictionScheduledExecutor.factory.equals(that.evictionScheduledExecutor.factory) : that.evictionScheduledExecutor.factory != null)
         return false;
      if (evictionScheduledExecutor.properties != null ? !evictionScheduledExecutor.properties.equals(that.evictionScheduledExecutor.properties) : that.evictionScheduledExecutor.properties != null)
         return false;
      if (serialization.marshallerClass != null ? !serialization.marshallerClass.equals(that.serialization.marshallerClass) : that.serialization.marshallerClass != null)
         return false;
      if (replicationQueueScheduledExecutor.factory != null ? !replicationQueueScheduledExecutor.factory.equals(that.replicationQueueScheduledExecutor.factory) : that.replicationQueueScheduledExecutor.factory != null)
         return false;
      if (replicationQueueScheduledExecutor.properties != null ? !replicationQueueScheduledExecutor.properties.equals(that.replicationQueueScheduledExecutor.properties) : that.replicationQueueScheduledExecutor.properties != null)
         return false;
      if (shutdown.hookBehavior != null ? !shutdown.hookBehavior.equals(that.shutdown.hookBehavior) : that.shutdown.hookBehavior != null)
         return false;
      if (transport.transportClass != null ? !transport.transportClass.equals(that.transport.transportClass) : that.transport.transportClass != null)
         return false;
      if (transport.properties != null ? !transport.properties.equals(that.transport.properties) : that.transport.properties != null)
         return false;
      return !(transport.distributedSyncTimeout != null && !transport.distributedSyncTimeout.equals(that.transport.distributedSyncTimeout));

   }

   @Override
   public int hashCode() {
      int result = asyncListenerExecutor.factory != null ? asyncListenerExecutor.factory.hashCode() : 0;
      result = 31 * result + (asyncListenerExecutor.properties != null ? asyncListenerExecutor.properties.hashCode() : 0);
      result = 31 * result + (asyncTransportExecutor.factory != null ? asyncTransportExecutor.factory.hashCode() : 0);
      result = 31 * result + (asyncTransportExecutor.properties != null ? asyncTransportExecutor.properties.hashCode() : 0);
      result = 31 * result + (evictionScheduledExecutor.factory != null ? evictionScheduledExecutor.factory.hashCode() : 0);
      result = 31 * result + (evictionScheduledExecutor.properties != null ? evictionScheduledExecutor.properties.hashCode() : 0);
      result = 31 * result + (replicationQueueScheduledExecutor.factory != null ? replicationQueueScheduledExecutor.factory.hashCode() : 0);
      result = 31 * result + (replicationQueueScheduledExecutor.properties != null ? replicationQueueScheduledExecutor.properties.hashCode() : 0);
      result = 31 * result + (serialization.marshallerClass != null ? serialization.marshallerClass.hashCode() : 0);
      result = 31 * result + (transport.transportClass != null ? transport.transportClass.hashCode() : 0);
      result = 31 * result + (transport.properties != null ? transport.properties.hashCode() : 0);     
      result = 31 * result + (transport.clusterName != null ? transport.clusterName.hashCode() : 0);
      result = 31 * result + (shutdown.hookBehavior.hashCode());
      result = 31 * result + serialization.version.hashCode();
      result = (int) (31 * result + transport.distributedSyncTimeout);
      return result;
   }

   @Override
   public GlobalConfiguration clone() {
      try {
         GlobalConfiguration dolly = (GlobalConfiguration) super.clone();
         if (asyncListenerExecutor != null) dolly.asyncListenerExecutor = asyncListenerExecutor.clone();
         if (asyncTransportExecutor != null) dolly.asyncTransportExecutor = asyncTransportExecutor.clone();
         if (evictionScheduledExecutor != null) dolly.evictionScheduledExecutor = evictionScheduledExecutor.clone();
         if (replicationQueueScheduledExecutor != null)
            dolly.replicationQueueScheduledExecutor = replicationQueueScheduledExecutor.clone();
         if (globalJmxStatistics != null)
            dolly.globalJmxStatistics = (GlobalJmxStatisticsType) globalJmxStatistics.clone();
         if (transport != null) dolly.transport = transport.clone();
         if (serialization != null) dolly.serialization = (SerializationType) serialization.clone();
         if (shutdown != null) dolly.shutdown = (ShutdownType) shutdown.clone();
         return dolly;
      }
      catch (CloneNotSupportedException e) {
         throw new CacheException("Problems cloning configuration component!", e);
      }
   }

   /**
    * Helper method that gets you a default constructed GlobalConfiguration, preconfigured to use the default clustering
    * stack.
    *
    * @return a new global configuration
    */
   public static GlobalConfiguration getClusteredDefault() {
      GlobalConfiguration gc = new GlobalConfiguration();
      gc.setTransportClass(JGroupsTransport.class.getName());
      gc.setTransportProperties((Properties) null);
      Properties p = new Properties();
      p.setProperty("threadNamePrefix", "asyncTransportThread");
      gc.setAsyncTransportExecutorProperties(p);
      return gc;
   }

   /**
    * Helper method that gets you a default constructed GlobalConfiguration, preconfigured for use in LOCAL mode
    *
    * @return a new global configuration
    */
   public static GlobalConfiguration getNonClusteredDefault() {
      GlobalConfiguration gc = new GlobalConfiguration();
      gc.setTransportClass(null);
      gc.setTransportProperties((Properties) null);
      return gc;
   }

   public abstract static class FactoryClassWithPropertiesType extends AbstractConfigurationBeanWithGCR {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 7625606997888180254L;
            
      @ConfigurationDocs( {
               @ConfigurationDoc(name = "maxThreads", 
                        desc = "Maximum number of threads for this executor. Default values can be found <a href=&quot;http://community.jboss.org/docs/DOC-15540&quot;>here</a>"),
               @ConfigurationDoc(name = "threadNamePrefix", 
                        desc = "Thread name prefix for threads created by this executor. Default values can be found <a href=&quot;http://community.jboss.org/docs/DOC-15540&quot;>here</a>") })
      protected TypedProperties properties = EMPTY_PROPERTIES;

      public void accept(ConfigurationBeanVisitor v) {
         v.visitFactoryClassWithPropertiesType(this);
      }

      public void setProperties(TypedProperties properties) {
         testImmutability("properties");
         this.properties = properties;
      }
      
      @XmlElement(name = "properties")
      public TypedProperties getProperties(){
         return properties;
      }

      @Override
      public FactoryClassWithPropertiesType clone() throws CloneNotSupportedException {
         FactoryClassWithPropertiesType dolly = (FactoryClassWithPropertiesType) super.clone();
         dolly.properties = (TypedProperties) properties.clone();
         return dolly;
      }
   }

   /**
    * 
    * @see <a href="../../../config.html#ce_global_asyncListenerExecutor">Configuration reference</a>
    * 
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDocs( {
            @ConfigurationDoc(name = "asyncListenerExecutor", 
                     desc = "Configuration for the executor service used to emit notifications to asynchronous listeners"),
            @ConfigurationDoc(name = "asyncTransportExecutor",
                     desc = "Configuration for the executor service used for asynchronous work on the Transport, including asynchronous marshalling and Cache 'async operations' such as Cache.putAsync().") })
   public static class ExecutorFactoryType extends FactoryClassWithPropertiesType implements ExecutorFactoryConfig<ExecutorFactory>{

      private static final long serialVersionUID = 6895901500645539386L;
                  
      @ConfigurationDoc(name="factory", desc="Fully qualified class name of the ExecutorFactory to use.  Must implement org.infinispan.executors.ExecutorFactory")
      protected String factory = DefaultExecutorFactory.class.getName();

      public ExecutorFactoryType(String factory) {
         this.factory = factory;
      }

      public ExecutorFactoryType() {
      }

      @XmlAttribute
      public void setFactory(String factory) {
         testImmutability("factory");
         this.factory = factory;
      }

      @Override
      public ExecutorFactoryType clone() throws CloneNotSupportedException {
         return (ExecutorFactoryType) super.clone();
      }

      @Override
      public ExecutorFactoryConfig<ExecutorFactory> factory(Class<? extends ExecutorFactory> clazz) {
         factory = clazz.getName();
         return this;
      }

      @Override
      public Object addProperty(String key, String value) {
         return getProperties().put(key, value);
      }            

      @Override
      public ExecutorFactoryConfig<ExecutorFactory> withProperties(Properties props) {
         getProperties().putAll(props);
         return this;
      }
   }

   /**
    * 
    * 
    * @see <a href="../../../config.html#ce_global_evictionScheduledExecutor">Configuration reference</a>
    * 
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDocs( {
            @ConfigurationDoc(name = "evictionScheduledExecutor", 
                     desc = "Configuration for the scheduled executor service used to periodically run eviction cleanup tasks."),
            @ConfigurationDoc(name = "replicationQueueScheduledExecutor", 
                     desc = "Configuration for the scheduled executor service used to periodically flush replication queues, used if asynchronous clustering is enabled along with useReplQueue being set to true.") })
   public static class ScheduledExecutorFactoryType extends FactoryClassWithPropertiesType implements ExecutorFactoryConfig<ScheduledExecutorFactory>{

      private static final long serialVersionUID = 7806391452092647488L;
            
      @ConfigurationDoc(name="factory",desc="Fully qualified class name of the ScheduledExecutorFactory to use.  Must implement org.infinispan.executors.ScheduledExecutorFactory")
      protected String factory = DefaultScheduledExecutorFactory.class.getName();

      public ScheduledExecutorFactoryType(String factory) {
         this.factory = factory;
      }

      public ScheduledExecutorFactoryType() {
      }

      @XmlAttribute
      public void setFactory(String factory) {
         testImmutability("factory");
         this.factory = factory;
      }

      @Override
      public ScheduledExecutorFactoryType clone() throws CloneNotSupportedException {
         return (ScheduledExecutorFactoryType) super.clone();
      }

      @Override
      public ExecutorFactoryConfig<ScheduledExecutorFactory> factory(Class<? extends ScheduledExecutorFactory> clazz) {
         testImmutability("factory");
         this.factory = clazz.getName();
         return this;
      }

      @Override
      public Object addProperty(String key, String value) {
         return getProperties().put(key, value);
      }

      @Override      
      public ExecutorFactoryConfig<ScheduledExecutorFactory> withProperties(Properties props) {
         getProperties().putAll(props);
         return this;
      }
   }

   /**
    * This element configures the transport used for network communications across the cluster.
    * 
    * @see <a href="../../../config.html#ce_global_transport">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name="transport")
   public static class TransportType extends AbstractConfigurationBeanWithGCR  implements TransportConfig{

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = -4739815717370060368L;
     
      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setClusterName")      
      protected String clusterName = "Infinispan-Cluster";

      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setMachineId")
      protected String machineId;

      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setRackId")      
      protected String rackId;

      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setSiteId")      
      protected String siteId;

      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setStrictPeerToPeer")
      protected Boolean strictPeerToPeer = true;      
      
      @ConfigurationDoc(name="distributedSyncTimeout",desc="Cluster-wide synchronization timeout for locks.  Used to coordinate changes in cluster membership.")
      protected Long distributedSyncTimeout = 240000L; // default
      
      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setTransportClass")
      protected String transportClass = null; // The default constructor sets default to JGroupsTransport

      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setTransportNodeName")
      protected String nodeName = null;

      @XmlElement(name = "properties")
      protected TypedProperties properties = EMPTY_PROPERTIES;

      public TransportType() {
         super();
         transportClass = JGroupsTransport.class.getName();
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitTransportType(this);
      }

      public TransportType(String transportClass) {
         super();
         this.transportClass = transportClass;
      }

      
      @Override
      public TransportConfig clusterName(String clusterName) {
         testImmutability("clusterName");
         this.clusterName = clusterName;
         return this;
      }

      
      @Override
      public TransportConfig machineId(String machineId) {
         testImmutability("machineId");
         this.machineId = machineId;
         return this;
      }

      
      @Override
      public TransportConfig rackId(String rackId) {
         testImmutability("rackId");
         this.rackId = rackId;
         return this;
      }

      
      @Override
      public TransportConfig siteId(String siteId) {
         testImmutability("siteId");
         this.siteId = siteId;
         return this;
      }
      
      @XmlAttribute
      public void setClusterName(String clusterName) {
         testImmutability("clusterName");
         this.clusterName = clusterName;
      }

      @XmlAttribute
      public void setMachineId(String machineId) {
         testImmutability("machineId");
         this.machineId = machineId;
      }

      @XmlAttribute
      public void setRackId(String rackId) {
         testImmutability("rackId");
         this.rackId = rackId;
      }

      @XmlAttribute
      public void setSiteId(String siteId) {
         testImmutability("siteId");
         this.siteId = siteId;
      }

      @Override
      public TransportConfig distributedSyncTimeout(Long distributedSyncTimeout) {
         testImmutability("distributedSyncTimeout");
         this.distributedSyncTimeout = distributedSyncTimeout;
         return this;
      }
      
      @XmlAttribute      
      public void setDistributedSyncTimeout(Long distributedSyncTimeout) {
         testImmutability("distributedSyncTimeout");
         this.distributedSyncTimeout = distributedSyncTimeout;
      }

      @XmlAttribute     
      public void setTransportClass(String transportClass) {
         testImmutability("transportClass");
         this.transportClass = transportClass;
      }
      
      @Override
      public TransportConfig transportClass(Class<? extends Transport> transportClass) {
         testImmutability("transportClass");
         this.transportClass = transportClass.getName();
         return this;        
      }

      
      @Override
      public TransportConfig nodeName(String nodeName) {
         testImmutability("nodeName");
         this.nodeName = nodeName;
         return this;
      }
      
      @XmlAttribute
      public void setNodeName(String nodeName) {
         testImmutability("nodeName");
         this.nodeName = nodeName;
      }
      
      @XmlTransient
      public void setProperties(TypedProperties properties) {
         testImmutability("properties");
         this.properties = properties;
      }
      
      @Override
      public TransportConfig withProperties(Properties properties) {
         testImmutability("properties");
         this.properties = toTypedProperties(properties);
         return this;
      }
      
      @Override
      public Object addProperty(String key, String value){
         return this.properties.put(key, value);
      }
      
      @Override
      public TransportConfig strictPeerToPeer(Boolean strictPeerToPeer) {
         testImmutability("strictPeerToPeer");
         this.strictPeerToPeer = strictPeerToPeer;
         return this;
      }
      
      @XmlAttribute      
      public void setStrictPeerToPeer(Boolean strictPeerToPeer) {
         testImmutability("strictPeerToPeer");
         this.strictPeerToPeer = strictPeerToPeer;
      }

      @Override
      public TransportType clone() throws CloneNotSupportedException {
         TransportType dolly = (TransportType) super.clone();
         dolly.properties = (TypedProperties) properties.clone();
         return dolly;
      }      
   }

   /**
    * Serialization and marshalling settings.
    * 
    * @see <a href="../../../config.html#ce_global_serialization">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name="serialization")
   public static class SerializationType extends AbstractConfigurationBeanWithGCR implements SerializationConfig{

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = -925947118621507282L;

      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setMarshallerClass")      
      protected String marshallerClass = VersionAwareMarshaller.class.getName(); // the default
      
      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setMarshallVersion")      
      protected String version = Version.MAJOR_MINOR;

      @XmlElement(name = "externalizers")
      protected ExternalizersType externalizerTypes = new ExternalizersType();

      public SerializationType() {
         super();
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitSerializationType(this);
      }
      
      public void setExternalizerTypes(ExternalizersType externalizerTypes) {
         this.externalizerTypes = externalizerTypes;
      }

      @Override
      public SerializationConfig marshallerClass(Class<? extends Marshaller> marshallerClass) {
         testImmutability("marshallerClass");
         this.marshallerClass = marshallerClass.getName();
         return this;
      }

      
      @Override
      public SerializationConfig version(String version) {
         testImmutability("version");
         this.version = version;
         return this;
      }
      
      @XmlAttribute
      public void setMarshallerClass(String marshallerClass) {
         testImmutability("marshallerClass");
         this.marshallerClass = marshallerClass;
      }

      @XmlAttribute
      public void setVersion(String version) {
         testImmutability("version");
         this.version = version;
      }
      
      // TODO implement equals and hashCode and update parent equals/hashcode

      @Override
      public ExternalizersConfig configureExternalizers() {
         return externalizerTypes;
      }
   }
   /**
    * Configures custom marshallers.
    *
    * @see <a href="../../../config.html#ce_global_serialization_marshallers">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.FIELD)
   @ConfigurationDoc(name = "externalizers")
   public static class ExternalizersType extends AbstractConfigurationBeanWithGCR implements ExternalizersConfig {
      

      /** The serialVersionUID */
      private static final long serialVersionUID = -496116709223466807L;
      
      @XmlElement(name = "externalizer")
      private List<ExternalizerConfig> externalizers = new ArrayList<ExternalizerConfig>();

      @Override
      public ExternalizersType clone() throws CloneNotSupportedException {
         ExternalizersType dolly = (ExternalizersType) super.clone();
         if (externalizers != null) {
            dolly.externalizers = new ArrayList<ExternalizerConfig>();
            for (ExternalizerConfig config : externalizers) {
               ExternalizerConfig clone = (ExternalizerConfig) config.clone();
               dolly.externalizers.add(clone);
            }
         }
         return dolly;
      }

      public void accept(ConfigurationBeanVisitor v) {
         for (ExternalizerConfig i : externalizers) {
            i.accept(v);
         }
         v.visitExternalizersType(this);
      }

      public List<ExternalizerConfig> getExternalizerConfigs() {
         return externalizers;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof ExternalizersType)) return false;

         ExternalizersType that = (ExternalizersType) o;

         if (externalizers != null ? !externalizers.equals(that.externalizers) : that.externalizers != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         return externalizers != null ? externalizers.hashCode() : 0;
      }

      @Deprecated
      public void setExternalizerConfigs(List<ExternalizerConfig> externalizers) {
         testImmutability("externalizers");
         this.externalizers = externalizers;
      }

      @Override
      public ExternalizersConfig addExtrenalizer(ExternalizerConfig e) {   
         this.externalizers.add(e);
         return this;
      }

      @Override
      public <T> ExternalizersConfig addExtrenalizer(Class<? extends Externalizer<T>> clazz, int id) {
         ExternalizerConfig ec = new ExternalizerConfig();
         ec.setExternalizerClass(clazz.getName());
         ec.setId(id);
         addExtrenalizer(ec);
         return this;
      }
   }

   /**
    * This element specifies whether global statistics are gathered and reported via JMX for all
    * caches under this cache manager.
    * 
    * @see <a href="../../../config.html#ce_global_globalJmxStatistics">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "globalJmxStatistics")
   public static class GlobalJmxStatisticsType extends AbstractConfigurationBeanWithGCR implements
            GlobalJmxStatisticsConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 6639689526822921024L;

      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setExposeGlobalJmxStatistics")
      protected Boolean enabled = false;

      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setJmxDomain")
      protected String jmxDomain = "org.infinispan";

      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setMBeanServerLookup")
      protected String mBeanServerLookup = PlatformMBeanServerLookup.class.getName();

      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setAllowDuplicateDomains")
      protected Boolean allowDuplicateDomains = false;

      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setCacheManagerName")
      protected String cacheManagerName = "DefaultCacheManager";

      @XmlElement(name = "properties")
      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setMBeanServerProperties")
      protected TypedProperties properties = EMPTY_PROPERTIES;
      
      private MBeanServerLookup mBeanServerLookupInstance;

      @Override
      public GlobalJmxStatisticsConfig enabled(Boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
         return this;
      }

      @XmlAttribute
      public void setEnabled(Boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitGlobalJmxStatisticsType(this);
      }

      @XmlTransient
      public void setProperties(TypedProperties p) {
         this.properties = p;
      }

      @Override
      public GlobalJmxStatisticsConfig jmxDomain(String jmxDomain) {
         testImmutability("jmxDomain");
         this.jmxDomain = jmxDomain;
         return this;
      }

      @XmlAttribute
      public void setJmxDomain(String jmxDomain) {
         testImmutability("jmxDomain");
         this.jmxDomain = jmxDomain;
      }

      @Override
      public GlobalJmxStatisticsConfig mBeanServerLookup(Class<? extends MBeanServerLookup> beanServerLookupClass) {
         testImmutability("mBeanServerLookup");
         mBeanServerLookup = beanServerLookupClass.getName();
         return this;
      }

      @XmlAttribute
      public void setMBeanServerLookup(String beanServerLookup) {
         testImmutability("mBeanServerLookup");
         mBeanServerLookup = beanServerLookup;
      }

      @Override
      public GlobalJmxStatisticsConfig allowDuplicateDomains(Boolean allowDuplicateDomains) {
         testImmutability("allowDuplicateDomains");
         this.allowDuplicateDomains = allowDuplicateDomains;
         return this;
      }

      @XmlAttribute
      public void setAllowDuplicateDomains(Boolean allowDuplicateDomains) {
         testImmutability("allowDuplicateDomains");
         this.allowDuplicateDomains = allowDuplicateDomains;
      }

      @Override
      public GlobalJmxStatisticsConfig cacheManagerName(String cacheManagerName) {
         testImmutability("cacheManagerName");
         this.cacheManagerName = cacheManagerName;
         return this;
      }

      @XmlAttribute
      public void setCacheManagerName(String cacheManagerName) {
         testImmutability("cacheManagerName");
         this.cacheManagerName = cacheManagerName;
      }

      public MBeanServerLookup getMBeanServerLookupInstance() {
         if (mBeanServerLookupInstance == null)
            mBeanServerLookupInstance = (MBeanServerLookup) Util.getInstance(mBeanServerLookup);

         return mBeanServerLookupInstance;
      }

      @XmlTransient
      public void setMBeanServerLookupInstance(MBeanServerLookup MBeanServerLookupInstance) {
         this.mBeanServerLookupInstance = MBeanServerLookupInstance;
      }

      @Override
      public GlobalJmxStatisticsConfig setProperties(Properties p) {
         properties.putAll(p);
         return this;
      }

      @Override
      public GlobalJmxStatisticsConfig usingMBeanServerLookupInstance(MBeanServerLookup MBeanServerLookupInstance) {
         this.mBeanServerLookupInstance = MBeanServerLookupInstance;
         return this;
      }
   }

   /**
    * This element specifies behavior when the JVM running the cache instance shuts down.
    * 
    * @see <a href="../../../config.html#ce_global_shutdown">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name="shutdown")
   public static class ShutdownType extends AbstractConfigurationBeanWithGCR {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 3427920991221031456L;

      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setShutdownHookBehavior")
      protected ShutdownHookBehavior hookBehavior = ShutdownHookBehavior.DEFAULT;

      @XmlAttribute
      public void setHookBehavior(ShutdownHookBehavior hookBehavior) {
         testImmutability("hookBehavior");
         this.hookBehavior = hookBehavior;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitShutdownType(this);
      }
   }
}

abstract class AbstractConfigurationBeanWithGCR extends AbstractConfigurationBean {

   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = -5124687543159561028L;

   GlobalComponentRegistry gcr = null;

   @Inject
   public void inject(GlobalComponentRegistry gcr) {
      this.gcr = gcr;
   }

   @Override
   protected boolean hasComponentStarted() {
      return gcr != null && gcr.getStatus() != null && gcr.getStatus() == ComponentStatus.RUNNING;
   }

   @Override
   public CloneableConfigurationComponent clone() throws CloneNotSupportedException {
      AbstractConfigurationBeanWithGCR dolly = (AbstractConfigurationBeanWithGCR) super.clone();
      // Do not clone the registry to avoid leak of runtime information to clone users
      dolly.gcr = null;
      return dolly;
   }
}

class PropertiesType {

   @XmlElement(name = "property")
   Property properties[];
}

class Property {

   @XmlAttribute
   String name;

   @XmlAttribute
   String value;
}
