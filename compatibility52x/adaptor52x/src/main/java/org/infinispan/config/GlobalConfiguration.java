package org.infinispan.config;

import org.infinispan.CacheException;
import org.infinispan.Version;
import org.infinispan.commons.executors.ExecutorFactory;
import org.infinispan.config.FluentGlobalConfiguration.ExecutorFactoryConfig;
import org.infinispan.config.FluentGlobalConfiguration.GlobalJmxStatisticsConfig;
import org.infinispan.config.FluentGlobalConfiguration.SerializationConfig;
import org.infinispan.config.FluentGlobalConfiguration.ShutdownConfig;
import org.infinispan.config.FluentGlobalConfiguration.TransportConfig;
import org.infinispan.executors.DefaultExecutorFactory;
import org.infinispan.executors.DefaultScheduledExecutorFactory;
import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.MBeanServerLookup;
import org.infinispan.jmx.PlatformMBeanServerLookup;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.Marshaller;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jboss.marshalling.ClassResolver;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * Configuration component that encapsulates the global configuration.
 * <p/>
 *
 * A default instance of this bean takes default values for each attribute.  Please see the individual setters for
 * details of what these defaults are.
 * <p/>
 * @deprecated This class is deprecated.  Use {@link org.infinispan.configuration.global.GlobalConfiguration} instead.
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
@XmlRootElement
@ConfigurationDoc(name = "global", desc = "Defines global settings shared among all cache instances created by a single CacheManager.")
@Deprecated
public class GlobalConfiguration extends AbstractConfigurationBean {

   private static final Log log = LogFactory.getLog(GlobalConfiguration.class);

   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = 8910865501990177720L;

   /**
    * Default replication version, from {@link org.infinispan.Version#getVersionShort}.
    */
   public static final short DEFAULT_MARSHALL_VERSION = Version.getVersionShort();

   @XmlTransient
   FluentGlobalConfiguration fluentGlobalConfig = new FluentGlobalConfiguration(this);

   @XmlElement
   ExecutorFactoryType asyncListenerExecutor = new ExecutorFactoryType().setGlobalConfiguration(this);

   @XmlElement
   ExecutorFactoryType asyncTransportExecutor = new ExecutorFactoryType().setGlobalConfiguration(this);

   @XmlElement
   ScheduledExecutorFactoryType evictionScheduledExecutor = new ScheduledExecutorFactoryType().setGlobalConfiguration(this);

   @XmlElement
   ScheduledExecutorFactoryType replicationQueueScheduledExecutor = new ScheduledExecutorFactoryType().setGlobalConfiguration(this);

   @XmlElement
   GlobalJmxStatisticsType globalJmxStatistics = new GlobalJmxStatisticsType().setGlobalConfiguration(this);

   @XmlElement
   TransportType transport = new TransportType(null).setGlobalConfiguration(this);

   @XmlElement
   SerializationType serialization = new SerializationType().setGlobalConfiguration(this);

   @XmlElement
   ShutdownType shutdown = new ShutdownType().setGlobalConfiguration(this);

   @XmlTransient
   GlobalComponentRegistry gcr;
   
   @XmlTransient
   private final ClassLoader cl;

   /**
    * Create a new GlobalConfiguration, using the Thread Context ClassLoader to load any
    * classes or resources required by this configuration. The TCCL will also be used as
    * default classloader for the CacheManager and any caches created.
    * 
    */
   public GlobalConfiguration() {
      this(Thread.currentThread().getContextClassLoader());
   }
   
   /**
    * Create a new GlobalConfiguration, specifying the classloader to use. This classloader will
    * be used to load resources or classes required by configuration, and used as the default
    * classloader for the CacheManager and any caches created.
    * 
    * @param cl
    */
   public GlobalConfiguration(ClassLoader cl) {
      super();
      if (cl == null)
         throw new IllegalArgumentException("cl must not be null");
      this.cl = cl;
   }

   /**
    * Use the {@link org.infinispan.configuration.global.GlobalConfigurationBuilder}
    * hierarchy to configure Infinispan cache managers fluently.
    */
   @Deprecated
   public FluentGlobalConfiguration fluent() {
      return fluentGlobalConfig;
   }

   public boolean isExposeGlobalJmxStatistics() {
      return globalJmxStatistics.enabled;
   }

   /**
    * Toggle to enable/disable global statistics being exported via JMX
    *
    * @param exposeGlobalJmxStatistics
    * @deprecated Use {@link FluentGlobalConfiguration#globalJmxStatistics()} instead
    */
   @Deprecated
   public void setExposeGlobalJmxStatistics(boolean exposeGlobalJmxStatistics) {
      testImmutability("exposeGlobalManagementStatistics");
      globalJmxStatistics.setEnabled(exposeGlobalJmxStatistics);
   }

   /**
    * If JMX statistics are enabled then all 'published' JMX objects will appear under this name.
    * This is optional, if not specified an object name will be created for you by default.
    *
    * @param jmxObjectName
    * @deprecated Use {@link FluentGlobalConfiguration.GlobalJmxStatisticsConfig#jmxDomain(String)} instead
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
    *
    * @param properties properties to pass to the MBean Server Lookup
    * @deprecated Use {@link FluentGlobalConfiguration.GlobalJmxStatisticsConfig#withProperties(java.util.Properties)} or
    * {@link FluentGlobalConfiguration.GlobalJmxStatisticsConfig#addProperty(String, String)} instead
    */
   @Deprecated
   public void setMBeanServerProperties(Properties properties) {
      globalJmxStatistics.setProperties(toTypedProperties(properties));
   }

   /**
    * Fully qualified name of class that will attempt to locate a JMX MBean server to bind to
    *
    * @param mBeanServerLookupClass fully qualified class name of the MBean Server Lookup class implementation
    * @deprecated Use {@link FluentGlobalConfiguration.GlobalJmxStatisticsConfig#mBeanServerLookupClass(Class)} instead
    */
   @Deprecated
   public void setMBeanServerLookup(String mBeanServerLookupClass) {
      globalJmxStatistics.setMBeanServerLookup(mBeanServerLookupClass);
   }

   /**
    * @deprecated Use {@link FluentGlobalConfiguration.GlobalJmxStatisticsConfig#mBeanServerLookup(org.infinispan.jmx.MBeanServerLookup)} instead
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
    * @deprecated Use {@link FluentGlobalConfiguration.GlobalJmxStatisticsConfig#mBeanServerLookup(org.infinispan.jmx.MBeanServerLookup)} instead
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
    * @deprecated Use {@link FluentGlobalConfiguration.GlobalJmxStatisticsConfig#allowDuplicateDomains(Boolean)} instead
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
    * @deprecated Use {@link FluentGlobalConfiguration.GlobalJmxStatisticsConfig#cacheManagerName(String)} instead
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
    * @deprecated Use {@link FluentGlobalConfiguration.TransportConfig#strictPeerToPeer(Boolean)} instead
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
   @Deprecated
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

   @Override
   protected boolean hasComponentStarted() {
      return gcr != null && gcr.getStatus() != null && gcr.getStatus() == ComponentStatus.RUNNING;
   }

   public String getAsyncListenerExecutorFactoryClass() {
      return asyncListenerExecutor.factory;
   }

   /**
    * @deprecated Use {@link FluentGlobalConfiguration.ExecutorFactoryConfig#asyncListenerExecutor()}  instead
    */
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

   /**
    * @deprecated Use {@link FluentGlobalConfiguration.ExecutorFactoryConfig#evictionScheduledExecutor()} instead
    */
   @Deprecated
   public void setEvictionScheduledExecutorFactoryClass(String evictionScheduledExecutorFactoryClass) {
      evictionScheduledExecutor.setFactory(evictionScheduledExecutorFactoryClass);
   }

   public String getReplicationQueueScheduledExecutorFactoryClass() {
      return replicationQueueScheduledExecutor.factory;
   }

   /**
    * @deprecated Use {@link FluentGlobalConfiguration.ExecutorFactoryConfig#replicationQueueScheduledExecutor()} instead
    */
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
    * @deprecated Use {@link FluentGlobalConfiguration.SerializationConfig#marshallerClass(Class)} instead
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
    * @deprecated Use {@link FluentGlobalConfiguration.TransportConfig#nodeName(String)} instead
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
    * @deprecated Use {@link FluentGlobalConfiguration.TransportConfig#transportClass(Class)} instead
    */
   @Deprecated
   public void setTransportClass(String transportClass) {
      transport.setTransportClass(transportClass);
   }

   public Properties getTransportProperties() {
      return transport.properties;
   }

   /**
    * @deprecated Use {@link FluentGlobalConfiguration.TransportConfig#withProperties(java.util.Properties)} or
    * {@link FluentGlobalConfiguration.TransportConfig#addProperty(String, String)} instead
    */
   @Deprecated
   public void setTransportProperties(Properties transportProperties) {
      transport.setProperties(toTypedProperties(transportProperties));
   }

   /**
    * @deprecated Use {@link FluentGlobalConfiguration.TransportConfig#withProperties(java.util.Properties)} or
    * {@link FluentGlobalConfiguration.TransportConfig#addProperty(String, String)} instead
    */
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
    * @deprecated Use {@link FluentGlobalConfiguration.TransportConfig#clusterName(String)} instead
    */
   @Deprecated
   public void setClusterName(String clusterName) {
      transport.setClusterName(clusterName);
   }

   /**
    * The id of the machine where this node runs. Used for <a href="http://community.jboss.org/wiki/DesigningServerHinting">server
    * hinting</a> .

    * @deprecated Use {@link FluentGlobalConfiguration.TransportConfig#machineId(String)} instead
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
    *
    * @deprecated Use {@link FluentGlobalConfiguration.TransportConfig#rackId(String)} instead
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
    *
    * @deprecated Use {@link FluentGlobalConfiguration.TransportConfig#siteId(String)} instead
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
    * @deprecated Use {@link FluentGlobalConfiguration.ShutdownConfig#hookBehavior(org.infinispan.config.GlobalConfiguration.ShutdownHookBehavior)} instead
    */
   @Deprecated
   public void setShutdownHookBehavior(ShutdownHookBehavior shutdownHookBehavior) {
      shutdown.setHookBehavior(shutdownHookBehavior);
   }

   /**
    * @deprecated Use {@link FluentGlobalConfiguration.ShutdownConfig#hookBehavior(org.infinispan.config.GlobalConfiguration.ShutdownHookBehavior)} instead
    */
   @Deprecated
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


   /**
    * @deprecated Use {@link FluentGlobalConfiguration.ExecutorFactoryConfig#asyncListenerExecutor()}  instead
    */
   @Deprecated
   public void setAsyncListenerExecutorProperties(Properties asyncListenerExecutorProperties) {
      asyncListenerExecutor.setProperties(toTypedProperties(asyncListenerExecutorProperties));
   }

   /**
    * @deprecated Use {@link FluentGlobalConfiguration.ExecutorFactoryConfig#asyncListenerExecutor()}  instead
    */
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

   /**
    */
   @Deprecated
   public void setAsyncTransportExecutorProperties(String asyncSerializationExecutorPropertiesString) {
      this.asyncTransportExecutor.setProperties(toTypedProperties(asyncSerializationExecutorPropertiesString));
   }

   public Properties getEvictionScheduledExecutorProperties() {
      return evictionScheduledExecutor.properties;
   }

   /**
    * @deprecated Use {@link FluentGlobalConfiguration.ExecutorFactoryConfig#evictionScheduledExecutor()} instead
    */
   @Deprecated
   public void setEvictionScheduledExecutorProperties(Properties evictionScheduledExecutorProperties) {
      evictionScheduledExecutor.setProperties(toTypedProperties(evictionScheduledExecutorProperties));
   }

   /**
    * @deprecated Use {@link FluentGlobalConfiguration.ExecutorFactoryConfig#evictionScheduledExecutor()} instead
    */
   @Deprecated
   public void setEvictionScheduledExecutorProperties(String evictionScheduledExecutorPropertiesString) {
      evictionScheduledExecutor.setProperties(toTypedProperties(evictionScheduledExecutorPropertiesString));
   }

   public Properties getReplicationQueueScheduledExecutorProperties() {
      return replicationQueueScheduledExecutor.properties;
   }

   /**
    * @deprecated Use {@link FluentGlobalConfiguration.ExecutorFactoryConfig#replicationQueueScheduledExecutor()} instead
    */
   @Deprecated
   public void setReplicationQueueScheduledExecutorProperties(Properties replicationQueueScheduledExecutorProperties) {
      this.replicationQueueScheduledExecutor.setProperties(toTypedProperties(replicationQueueScheduledExecutorProperties));
   }

   /**
    * @deprecated Use {@link FluentGlobalConfiguration.ExecutorFactoryConfig#replicationQueueScheduledExecutor()} instead
    */
   @Deprecated
   public void setReplicationQueueScheduledExecutorProperties(String replicationQueueScheduledExecutorPropertiesString) {
      this.replicationQueueScheduledExecutor.setProperties(toTypedProperties(replicationQueueScheduledExecutorPropertiesString));
   }

   public short getMarshallVersion() {
      return serialization.versionShort;
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
    * @deprecated Use {@link FluentGlobalConfiguration.SerializationConfig#version(short)} instead
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
    * @deprecated Use {@link FluentGlobalConfiguration.SerializationConfig#version(String)} instead
    */
   @Deprecated
   public void setMarshallVersion(String marshallVersion) {
      serialization.setVersion(marshallVersion);
   }

   public List<AdvancedExternalizerConfig> getExternalizers() {
      return serialization.externalizerTypes.advancedExternalizers;
   }

   public ClassResolver getClassResolver() {
      return serialization.classResolver;
   }

   public long getDistributedSyncTimeout() {
      return transport.distributedSyncTimeout;
   }

   /**
    * @deprecated Use {@link FluentGlobalConfiguration.TransportConfig#distributedSyncTimeout(Long)} instead
    */
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
         if (asyncListenerExecutor != null) {
            dolly.asyncListenerExecutor = asyncListenerExecutor.clone();
            dolly.asyncListenerExecutor.setGlobalConfiguration(dolly);
         }
         if (asyncTransportExecutor != null) {
            dolly.asyncTransportExecutor = asyncTransportExecutor.clone();
            dolly.asyncTransportExecutor.setGlobalConfiguration(dolly);
         }
         if (evictionScheduledExecutor != null) {
            dolly.evictionScheduledExecutor = evictionScheduledExecutor.clone();
            dolly.evictionScheduledExecutor.setGlobalConfiguration(dolly);
         }
         if (replicationQueueScheduledExecutor != null) {
            dolly.replicationQueueScheduledExecutor = replicationQueueScheduledExecutor.clone();
            dolly.evictionScheduledExecutor.setGlobalConfiguration(dolly);
         }
         if (globalJmxStatistics != null) {
            dolly.globalJmxStatistics = (GlobalJmxStatisticsType) globalJmxStatistics.clone();
            dolly.globalJmxStatistics.setGlobalConfiguration(dolly);
         }
         if (transport != null) {
            dolly.transport = transport.clone();
            dolly.transport.setGlobalConfiguration(dolly);
         }
         if (serialization != null) {
            dolly.serialization = (SerializationType) serialization.clone();
            dolly.serialization.setGlobalConfiguration(dolly);
         }
         if (shutdown != null) {
            dolly.shutdown = (ShutdownType) shutdown.clone();
            dolly.shutdown.setGlobalConfiguration(dolly);
         }
         dolly.fluentGlobalConfig = new FluentGlobalConfiguration(dolly);
         return dolly;
      } catch (CloneNotSupportedException e) {
         throw new CacheException("Problems cloning configuration component!", e);
      }
   }

   /**
    * Converts this configuration instance to an XML representation containing the current settings.
    *
    * @return a string containing the formatted XML representation of this configuration instance.
    */
   public String toXmlString() {
      return InfinispanConfiguration.toXmlString(this);
   }

   /**
    * Helper method that gets you a default constructed GlobalConfiguration, preconfigured to use the default clustering
    * stack.
    *
    * @return a new global configuration
    */
   public static GlobalConfiguration getClusteredDefault(ClassLoader cl) {
      GlobalConfiguration gc =
            cl == null ? new GlobalConfiguration() : new GlobalConfiguration(cl);
      gc.setTransportClass(JGroupsTransport.class.getName());
      gc.setTransportProperties((Properties) null);
      Properties p = new Properties();
      p.setProperty("threadNamePrefix", "asyncTransportThread");
      gc.setAsyncTransportExecutorProperties(p);
      return gc;
   }

   /**
    * Helper method that gets you a default constructed GlobalConfiguration, preconfigured to use the default clustering
    * stack.
    *
    * @return a new global configuration
    */
   public static GlobalConfiguration getClusteredDefault() {
      return getClusteredDefault(null);
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
   
   /**
    * Get the classloader in use by this configuration.
    * 
    * @return
    */
   public ClassLoader getClassLoader() {
      return cl;
   }

   public abstract static class FactoryClassWithPropertiesType extends AbstractConfigurationBeanWithGCR {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 7625606997888180254L;

      @ConfigurationDocs({
              @ConfigurationDoc(name = "maxThreads",
                      desc = "Maximum number of threads for this executor. Default values can be found <a href=&quot;https://docs.jboss.org/author/display/ISPN/Default+Values+For+Property+Based+Attributes&quot;>here</a>"),
              @ConfigurationDoc(name = "threadNamePrefix",
                      desc = "Thread name prefix for threads created by this executor. Default values can be found <a href=&quot;https://docs.jboss.org/author/display/ISPN/Default+Values+For+Property+Based+Attributes&quot;>here</a>")})
      protected TypedProperties properties = new TypedProperties();

      public void accept(ConfigurationBeanVisitor v) {
         v.visitFactoryClassWithPropertiesType(this);
      }

      /**
       * @deprecated Visibility will be reduced. Instead use {@link #addProperty(String, String)}  or {@link #withProperties(java.util.Properties)} instead
       */
      @Deprecated
      public void setProperties(TypedProperties properties) {
         testImmutability("properties");
         this.properties = properties;
      }

      public Object addProperty(String key, String value) {
         properties.setProperty(key, value);
         return this;
      }

      public Object withProperties(Properties props) {
         properties.putAll(props);
         return this;
      }

      @XmlElement(name = "properties")
      @Deprecated
      public TypedProperties getProperties() {
         return properties();
      }

      public TypedProperties properties() {
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
   @ConfigurationDocs({
           @ConfigurationDoc(name = "asyncListenerExecutor",
                   desc = "Configuration for the executor service used to emit notifications to asynchronous listeners"),
           @ConfigurationDoc(name = "asyncTransportExecutor",
                   desc = "Configuration for the executor service used for asynchronous work on the Transport, including asynchronous marshalling and Cache 'async operations' such as Cache.putAsync().")})
   @Deprecated public static class ExecutorFactoryType extends FactoryClassWithPropertiesType implements ExecutorFactoryConfig<ExecutorFactory> {

      private static final long serialVersionUID = 6895901500645539386L;

      @ConfigurationDoc(name = "factory", desc = "Fully qualified class name of the ExecutorFactory to use.  Must implement org.infinispan.executors.ExecutorFactory")
      protected String factory = DefaultExecutorFactory.class.getName();

      public ExecutorFactoryType(String factory) {
         this.factory = factory;
      }

      public ExecutorFactoryType() {
      }

      public String factory() {
         return factory;
      }

      @XmlAttribute
      @Deprecated
      public String getFactory() {
         return factory();
      }

      public void setFactory(String factory) {
         testImmutability("factory");
         this.factory = factory;
      }

      @Override
      public ExecutorFactoryConfig<ExecutorFactory> factory(Class<? extends ExecutorFactory> clazz) {
         setFactory(clazz == null ? null : clazz.getName());
         return this;
      }

      @Override
      public ExecutorFactoryType clone() throws CloneNotSupportedException {
         return (ExecutorFactoryType) super.clone();
      }

      @Override
      public ExecutorFactoryConfig<ExecutorFactory> addProperty(String key, String value) {
         super.addProperty(key, value);
         return this;
      }

      @Override
      public ExecutorFactoryConfig<ExecutorFactory> withProperties(Properties props) {
         super.withProperties(props);
         return this;
      }

      @Override
      ExecutorFactoryType setGlobalConfiguration(GlobalConfiguration globalConfig) {
         super.setGlobalConfiguration(globalConfig);
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
   @ConfigurationDocs({
           @ConfigurationDoc(name = "evictionScheduledExecutor",
                   desc = "Configuration for the scheduled executor service used to periodically run eviction cleanup tasks."),
           @ConfigurationDoc(name = "replicationQueueScheduledExecutor",
                   desc = "Configuration for the scheduled executor service used to periodically flush replication queues, used if asynchronous clustering is enabled along with useReplQueue being set to true.")})
   @Deprecated public static class ScheduledExecutorFactoryType extends FactoryClassWithPropertiesType implements ExecutorFactoryConfig<ScheduledExecutorFactory> {

      private static final long serialVersionUID = 7806391452092647488L;

      @ConfigurationDoc(name = "factory", desc = "Fully qualified class name of the ScheduledExecutorFactory to use.  Must implement org.infinispan.executors.ScheduledExecutorFactory")
      protected String factory = DefaultScheduledExecutorFactory.class.getName();

      public ScheduledExecutorFactoryType(String factory) {
         this.factory = factory;
      }

      public ScheduledExecutorFactoryType() {
      }


      @XmlAttribute
      @Deprecated
      public String getFactory() {
         return factory;
      }

      public String factory() {
         return factory;
      }

      public void setFactory(String factory) {
         testImmutability("factory");
         this.factory = factory;
      }

      @Override
      public ExecutorFactoryConfig<ScheduledExecutorFactory> factory(Class<? extends ScheduledExecutorFactory> clazz) {
         setFactory(clazz == null ? null : clazz.getName());
         return this;
      }


      @Override
      public ScheduledExecutorFactoryType clone() throws CloneNotSupportedException {
         return (ScheduledExecutorFactoryType) super.clone();
      }

      @Override
      public ExecutorFactoryConfig<ScheduledExecutorFactory> addProperty(String key, String value) {
         super.addProperty(key, value);
         return this;
      }

      @Override
      public ExecutorFactoryConfig<ScheduledExecutorFactory> withProperties(Properties props) {
         super.withProperties(props);
         return this;
      }

      @Override
      ScheduledExecutorFactoryType setGlobalConfiguration(GlobalConfiguration globalConfig) {
         super.setGlobalConfiguration(globalConfig);
         return this;
      }
   }

   /**
    * This element configures the transport used for network communications across the cluster.
    *
    * @see <a href="../../../config.html#ce_global_transport">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "transport")
   @SuppressWarnings("boxing")
   @Deprecated public static class TransportType extends AbstractConfigurationBeanWithGCR implements TransportConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = -4739815717370060368L;

      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setClusterName")
      protected String clusterName = "ISPN";

      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setMachineId")
      protected String machineId;

      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setRackId")
      protected String rackId;

      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setSiteId")
      protected String siteId;

      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setStrictPeerToPeer")
      protected Boolean strictPeerToPeer = false;

      @ConfigurationDoc(name="distributedSyncTimeout",
                        desc="Hijacked to use as timeout for view installation tasks")
      protected Long distributedSyncTimeout = 60000L; // default

      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setTransportClass")
      protected String transportClass = null; // The default constructor sets default to JGroupsTransport

      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setTransportNodeName")
      protected String nodeName = null;

      @XmlElement(name = "properties")
      protected TypedProperties properties = new TypedProperties();

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


      @XmlAttribute
      public String getClusterName() {
         return clusterName;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #clusterName(String)} instead
       */
      @Deprecated
      public void setClusterName(String clusterName) {
         testImmutability("clusterName");
         this.clusterName = clusterName;
      }

      @Override
      public TransportConfig clusterName(String clusterName) {
         setClusterName(clusterName);
         return this;
      }


      @XmlAttribute
      public String getMachineId() {
         return machineId;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #machineId(String)} instead
       */
      @Deprecated
      public void setMachineId(String machineId) {
         testImmutability("machineId");
         this.machineId = machineId;
      }

      @Override
      public TransportConfig machineId(String machineId) {
         setMachineId(machineId);
         return this;
      }


      @XmlAttribute
      public String getRackId() {
         return rackId;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #rackId(String)} instead
       */
      @Deprecated
      public void setRackId(String rackId) {
         testImmutability("rackId");
         this.rackId = rackId;
      }

      @Override
      public TransportConfig rackId(String rackId) {
         setRackId(rackId);
         return this;
      }


      @XmlAttribute
      public String getSiteId() {
         return siteId;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #siteId(String)} instead
       */
      @Deprecated
      public void setSiteId(String siteId) {
         testImmutability("siteId");
         this.siteId = siteId;
      }

      @Override
      public TransportConfig siteId(String siteId) {
         setSiteId(siteId);
         return this;
      }


      @XmlAttribute
      public Long getDistributedSyncTimeout() {
         return distributedSyncTimeout;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #distributedSyncTimeout(Long)} instead
       */
      @Deprecated
      public void setDistributedSyncTimeout(Long distributedSyncTimeout) {
         testImmutability("distributedSyncTimeout");
         this.distributedSyncTimeout = distributedSyncTimeout;
      }

      @Override
      public TransportConfig distributedSyncTimeout(Long distributedSyncTimeout) {
         setDistributedSyncTimeout(distributedSyncTimeout);
         return this;
      }


      @XmlAttribute
      public String getTransportClass() {
         return transportClass;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #transportClass(Class)} instead
       */
      @Deprecated
      public void setTransportClass(String transportClass) {
         testImmutability("transportClass");
         this.transportClass = transportClass;
      }

      @Override
      public TransportConfig transportClass(Class<? extends Transport> transportClass) {
         setTransportClass(transportClass == null ? null : transportClass.getName());
         return this;
      }


      @XmlAttribute
      public String getNodeName() {
         return nodeName;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #nodeName(String)} instead
       */
      @Deprecated
      public void setNodeName(String nodeName) {
         testImmutability("nodeName");
         this.nodeName = nodeName;
      }

      @Override
      public TransportConfig nodeName(String nodeName) {
         setNodeName(nodeName);
         return this;
      }


      /**
       * @deprecated The visibility of this will be reduced, use {@link #withProperties(java.util.Properties)} or {@link #addProperty(String, String)} instead
       */
      @Deprecated
      @XmlTransient
      public void setProperties(TypedProperties properties) {
         testImmutability("properties");
         this.properties = properties;
      }

      @Override
      public TransportConfig withProperties(Properties properties) {
         setProperties(toTypedProperties(properties));
         return this;
      }

      @Override
      public TransportConfig addProperty(String key, String value) {
         this.properties.put(key, value);
         return this;
      }

      @XmlAttribute
      public Boolean getStrictPeerToPeer() {
         return strictPeerToPeer;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #strictPeerToPeer(Boolean)} instead
       */
      @Deprecated
      public void setStrictPeerToPeer(Boolean strictPeerToPeer) {
         testImmutability("strictPeerToPeer");
         this.strictPeerToPeer = strictPeerToPeer;
      }

      @Override
      public TransportConfig strictPeerToPeer(Boolean strictPeerToPeer) {
         setStrictPeerToPeer(strictPeerToPeer);
         return this;
      }

      @Override
      public TransportType clone() throws CloneNotSupportedException {
         TransportType dolly = (TransportType) super.clone();
         dolly.properties = (TypedProperties) properties.clone();
         return dolly;
      }

      @Override
      TransportType setGlobalConfiguration(GlobalConfiguration globalConfig) {
         super.setGlobalConfiguration(globalConfig);
         return this;
      }
   }

   /**
    * Serialization and marshalling settings.
    *
    * @see <a href="../../../config.html#ce_global_serialization">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "serialization")
   @Deprecated public static class SerializationType extends AbstractConfigurationBeanWithGCR implements SerializationConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = -925947118621507282L;

      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setMarshallerClass")
      protected String marshallerClass = VersionAwareMarshaller.class.getName(); // the default

      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setMarshallVersion")
      protected String version = Version.MAJOR_MINOR;

      private short versionShort;

      @XmlElement(name = "advancedExternalizers")
      protected AdvancedExternalizersType externalizerTypes = new AdvancedExternalizersType();

      @XmlTransient
      private ClassResolver classResolver;

      public SerializationType() {
         super();
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitSerializationType(this);
      }

      @XmlAttribute
      public String getMarshallerClass() {
         return marshallerClass;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #marshallerClass(Class)} instead
       */
      @Deprecated
      public void setMarshallerClass(String marshallerClass) {
         testImmutability("marshallerClass");
         this.marshallerClass = marshallerClass;
      }

      @Override
      public SerializationConfig marshallerClass(Class<? extends Marshaller> marshallerClass) {
         setMarshallerClass(marshallerClass == null ? null : marshallerClass.getName());
         return this;
      }

      @XmlAttribute
      public String getVersion() {
         return version;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #version(String)} instead
       */
      @Deprecated
      public void setVersion(String version) {
         testImmutability("version");
         this.version = version;
      }

      @Override
      public SerializationConfig version(String version) {
         setVersion(version);
         return this;
      }

      @Override
      public SerializationConfig version(short marshallVersion) {
         versionShort = marshallVersion;
         return this;
      }

      // TODO implement equals and hashCode and update parent equals/hashcode

      @Override
      SerializationType setGlobalConfiguration(GlobalConfiguration globalConfig) {
         externalizerTypes.setGlobalConfiguration(globalConfig);
         super.setGlobalConfiguration(globalConfig);
         return this;
      }

      @Override
      public <T> SerializationConfig addAdvancedExternalizer(Class<? extends AdvancedExternalizer<T>> clazz) {
         addAdvancedExternalizer(Integer.MAX_VALUE, clazz);
         return this;
      }

      @Override
      public <T> SerializationConfig addAdvancedExternalizer(int id, Class<? extends AdvancedExternalizer<T>> clazz) {
         AdvancedExternalizerConfig ec = new AdvancedExternalizerConfig();
         ec.setExternalizerClass(clazz.getName());
         if (id != Integer.MAX_VALUE)
            ec.setId(id);
         externalizerTypes.addExternalizer(ec);
         return this;
      }

      @Override
      public <T> SerializationConfig addAdvancedExternalizer(AdvancedExternalizer<T>... advancedExternalizers) {
         for (AdvancedExternalizer<?> ext : advancedExternalizers)
            externalizerTypes.addExternalizer(new AdvancedExternalizerConfig().setAdvancedExternalizer(ext));
         return this;
      }

      @Override
      public <T> SerializationConfig addAdvancedExternalizer(int id, AdvancedExternalizer<T> advancedExternalizer) {
         externalizerTypes.addExternalizer(
               new AdvancedExternalizerConfig().setId(id).setAdvancedExternalizer(advancedExternalizer));
         return this;
      }

      @Override
      public SerializationConfig classResolver(ClassResolver classResolver) {
         this.classResolver = classResolver;
         return this;
      }
   }

   /**
    * Configures custom marshallers.
    *
    * @see <a href="../../../config.html#ce_global_serialization_marshallers">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.FIELD)
   @ConfigurationDoc(name = "advancedExternalizers")
   @Deprecated public static class AdvancedExternalizersType extends AbstractConfigurationBeanWithGCR {
      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = -496116709223466807L;

      @XmlElement(name = "advancedExternalizer")
      private List<AdvancedExternalizerConfig> advancedExternalizers = new LinkedList<AdvancedExternalizerConfig>();

      @Override
      public AdvancedExternalizersType clone() throws CloneNotSupportedException {
         AdvancedExternalizersType dolly = (AdvancedExternalizersType) super.clone();
         if (advancedExternalizers != null) {
            dolly.advancedExternalizers = new LinkedList<AdvancedExternalizerConfig>();
            for (AdvancedExternalizerConfig config : advancedExternalizers) {
               AdvancedExternalizerConfig clone = (AdvancedExternalizerConfig) config.clone();
               dolly.advancedExternalizers.add(clone);
            }
         }
         return dolly;
      }

      public void accept(ConfigurationBeanVisitor v) {
         for (AdvancedExternalizerConfig i : advancedExternalizers) {
            i.accept(v);
         }
         v.visitAdvancedExternalizersType(this);
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (!(o instanceof AdvancedExternalizersType)) return false;

         AdvancedExternalizersType that = (AdvancedExternalizersType) o;

         if (advancedExternalizers != null ? !advancedExternalizers.equals(that.advancedExternalizers) : that.advancedExternalizers != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         return advancedExternalizers != null ? advancedExternalizers.hashCode() : 0;
      }

      AdvancedExternalizersType addExternalizer(AdvancedExternalizerConfig e) {
         this.advancedExternalizers.add(e);
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
   @Deprecated public static class GlobalJmxStatisticsType extends AbstractConfigurationBeanWithGCR
         implements GlobalJmxStatisticsConfig {

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
      protected TypedProperties properties = new TypedProperties();

      private MBeanServerLookup mBeanServerLookupInstance;

      public void accept(ConfigurationBeanVisitor v) {
         v.visitGlobalJmxStatisticsType(this);
      }

      @XmlAttribute
      public Boolean getEnabled() {
         return enabled;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link FluentGlobalConfiguration#globalJmxStatistics()} instead
       */
      @Deprecated
      public void setEnabled(Boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #withProperties(java.util.Properties)} instead
       */
      @Deprecated
      @XmlTransient
      public void setProperties(TypedProperties p) {
         this.properties = p;
      }

      @Override
      public GlobalJmxStatisticsConfig withProperties(Properties p) {
         setProperties(toTypedProperties(p));
         return this;
      }

      @Override
      public GlobalJmxStatisticsConfig addProperty(String key, String value) {
         properties.setProperty(key, value);
         return this;
      }

      @XmlAttribute
      public String getJmxDomain() {
         return jmxDomain;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #jmxDomain(String)} instead
       */
      @Deprecated
      public void setJmxDomain(String jmxDomain) {
         testImmutability("jmxDomain");
         this.jmxDomain = jmxDomain;
      }

      @Override
      public GlobalJmxStatisticsConfig jmxDomain(String jmxDomain) {
         setJmxDomain(jmxDomain);
         return this;
      }


      @XmlAttribute
      public String getMBeanServerLookup() {
         return mBeanServerLookup;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #mBeanServerLookupClass(Class)} instead
       */
      @Deprecated
      public void setMBeanServerLookup(String beanServerLookup) {
         testImmutability("mBeanServerLookup");
         mBeanServerLookup = beanServerLookup;
      }

      @Override
      public GlobalJmxStatisticsConfig mBeanServerLookupClass(Class<? extends MBeanServerLookup> beanServerLookupClass) {
         setMBeanServerLookup(beanServerLookupClass == null ? null : beanServerLookupClass.getName());
         return this;
      }


      @XmlAttribute
      public Boolean getAllowDuplicateDomains() {
         return allowDuplicateDomains;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #allowDuplicateDomains(Boolean)} instead
       */
      @Deprecated
      public void setAllowDuplicateDomains(Boolean allowDuplicateDomains) {
         testImmutability("allowDuplicateDomains");
         this.allowDuplicateDomains = allowDuplicateDomains;
      }

      @Override
      public GlobalJmxStatisticsConfig allowDuplicateDomains(Boolean allowDuplicateDomains) {
         setAllowDuplicateDomains(allowDuplicateDomains);
         return this;
      }

      @XmlAttribute
      public String getCacheManagerName() {
         return cacheManagerName;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #cacheManagerName(String)} instead
       */
      @Deprecated
      public void setCacheManagerName(String cacheManagerName) {
         testImmutability("cacheManagerName");
         this.cacheManagerName = cacheManagerName;
      }

      @Override
      public GlobalJmxStatisticsConfig cacheManagerName(String cacheManagerName) {
         setCacheManagerName(cacheManagerName);
         return this;
      }

      @XmlTransient
      public MBeanServerLookup getMBeanServerLookupInstance() {
         if (mBeanServerLookupInstance == null)
            mBeanServerLookupInstance = (MBeanServerLookup) Util.getInstance(mBeanServerLookup, globalConfig.getClassLoader());

         return mBeanServerLookupInstance;
      }

      @Override
      public GlobalJmxStatisticsConfig disable() {
         setEnabled(false);
         return this;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #mBeanServerLookup(org.infinispan.jmx.MBeanServerLookup)} instead
       */
      @Deprecated
      public void setMBeanServerLookupInstance(MBeanServerLookup MBeanServerLookupInstance) {
         this.mBeanServerLookupInstance = MBeanServerLookupInstance;
      }

      @Override
      public GlobalJmxStatisticsConfig mBeanServerLookup(MBeanServerLookup MBeanServerLookupInstance) {
         this.mBeanServerLookupInstance = MBeanServerLookupInstance;
         return this;
      }

      @Override
      GlobalJmxStatisticsType setGlobalConfiguration(GlobalConfiguration globalConfig) {
         super.setGlobalConfiguration(globalConfig);
         return this;
      }
   }

   /**
    * This element specifies behavior when the JVM running the cache instance shuts down.
    *
    * @see <a href="../../../config.html#ce_global_shutdown">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name = "shutdown")
   @Deprecated public static class ShutdownType extends AbstractConfigurationBeanWithGCR implements ShutdownConfig {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 3427920991221031456L;

      @ConfigurationDocRef(bean = GlobalConfiguration.class, targetElement = "setShutdownHookBehavior")
      protected ShutdownHookBehavior hookBehavior = ShutdownHookBehavior.DEFAULT;

      public void accept(ConfigurationBeanVisitor v) {
         v.visitShutdownType(this);
      }

      @XmlAttribute
      public ShutdownHookBehavior getHookBehavior() {
         return hookBehavior;
      }

      /**
       * @deprecated The visibility of this will be reduced, use {@link #hookBehavior(org.infinispan.config.GlobalConfiguration.ShutdownHookBehavior)} instead
       */
      @Deprecated
      public void setHookBehavior(ShutdownHookBehavior hookBehavior) {
         testImmutability("hookBehavior");
         this.hookBehavior = hookBehavior;
      }

      @Override
      public ShutdownConfig hookBehavior(ShutdownHookBehavior hookBehavior) {
         setHookBehavior(hookBehavior);
         return this;
      }

      @Override
      ShutdownType setGlobalConfiguration(GlobalConfiguration globalConfig) {
         super.setGlobalConfiguration(globalConfig);
         return this;
      }
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
