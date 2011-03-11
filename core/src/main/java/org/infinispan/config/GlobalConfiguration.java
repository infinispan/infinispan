package org.infinispan.config;

import org.infinispan.CacheException;
import org.infinispan.Version;
import org.infinispan.executors.DefaultExecutorFactory;
import org.infinispan.executors.DefaultScheduledExecutorFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.MBeanServerLookup;
import org.infinispan.jmx.PlatformMBeanServerLookup;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.Util;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;
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

   public boolean isExposeGlobalJmxStatistics() {
      return globalJmxStatistics.enabled;
   }

   /**
    * Toggle to enable/disable global statistics being exported via JMX
    * 
    * @param exposeGlobalJmxStatistics
    */
   public void setExposeGlobalJmxStatistics(boolean exposeGlobalJmxStatistics) {
      testImmutability("exposeGlobalManagementStatistics");
      globalJmxStatistics.setEnabled(exposeGlobalJmxStatistics);
   }

   /**
    * If JMX statistics are enabled then all 'published' JMX objects will appear under this name.
    * This is optional, if not specified an object name will be created for you by default.
    * 
    * @param jmxObjectName
    */
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
   public void setMBeanServerProperties(Properties properties) {
      globalJmxStatistics.setProperties(toTypedProperties(properties));
   }

   /**
    * Fully qualified name of class that will attempt to locate a JMX MBean server to bind to
    * 
    * @param mBeanServerLookupClass fully qualified class name of the MBean Server Lookup class implementation
    */
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

   public void setAsyncListenerExecutorFactoryClass(String asyncListenerExecutorFactoryClass) {
      asyncListenerExecutor.setFactory(asyncListenerExecutorFactoryClass);
   }

   public String getAsyncTransportExecutorFactoryClass() {
      return asyncTransportExecutor.factory;
   }

   public void setAsyncTransportExecutorFactoryClass(String asyncTransportExecutorFactoryClass) {
      this.asyncTransportExecutor.setFactory(asyncTransportExecutorFactoryClass);
   }

   public String getEvictionScheduledExecutorFactoryClass() {
      return evictionScheduledExecutor.factory;
   }

   public void setEvictionScheduledExecutorFactoryClass(String evictionScheduledExecutorFactoryClass) {
      evictionScheduledExecutor.setFactory(evictionScheduledExecutorFactoryClass);
   }

   public String getReplicationQueueScheduledExecutorFactoryClass() {
      return replicationQueueScheduledExecutor.factory;
   }


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
   public void setTransportClass(String transportClass) {
      transport.setTransportClass(transportClass);
   }

   public Properties getTransportProperties() {
      return transport.properties;
   }

   public void setTransportProperties(Properties transportProperties) {
      transport.setProperties(toTypedProperties(transportProperties));
   }

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
   public void setClusterName(String clusterName) {
      transport.setClusterName(clusterName);
   }

   /**
    * The id of the machine where this node runs. Used for <a href="http://community.jboss.org/wiki/DesigningServerHinting">server
    * hinting</a> .
    */
   public void setMachineId(String machineId) {
      transport.setMachineId(machineId);
   }

   /**
    * @see #setMachineId(String)
    */
   public String getMachineId() {
      return transport.getMachineId();
   }

   /**
    * The id of the rack where this node runs. Used for <a href="http://community.jboss.org/wiki/DesigningServerHinting">server
    * hinting</a> .
    */
   public void setRackId(String rackId) {
      transport.setRackId(rackId);
   }

   /**
    * @see #setRackId(String)
    */
   public String getRackId() {
      return transport.getRackId();
   }

   /**
    * The id of the site where this node runs. Used for <a href="http://community.jboss.org/wiki/DesigningServerHinting">server
    * hinting</a> .
    */
   public void setSiteId(String siteId) {
      transport.setSiteId(siteId);
   }

   /**
    * @see #setSiteId(String) 
    */
   public String getSiteId() {
      return transport.getSiteId();
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


   public void setAsyncListenerExecutorProperties(Properties asyncListenerExecutorProperties) {
      asyncListenerExecutor.setProperties(toTypedProperties(asyncListenerExecutorProperties));
   }

   public void setAsyncListenerExecutorProperties(String asyncListenerExecutorPropertiesString) {
      asyncListenerExecutor.setProperties(toTypedProperties(asyncListenerExecutorPropertiesString));
   }

   public Properties getAsyncTransportExecutorProperties() {
      return asyncTransportExecutor.properties;
   }

   public void setAsyncTransportExecutorProperties(Properties asyncTransportExecutorProperties) {
      this.asyncTransportExecutor.setProperties(toTypedProperties(asyncTransportExecutorProperties));
   }

   public void setAsyncTransportExecutorProperties(String asyncSerializationExecutorPropertiesString) {
      this.asyncTransportExecutor.setProperties(toTypedProperties(asyncSerializationExecutorPropertiesString));
   }

   public Properties getEvictionScheduledExecutorProperties() {
      return evictionScheduledExecutor.properties;
   }

   public void setEvictionScheduledExecutorProperties(Properties evictionScheduledExecutorProperties) {
      evictionScheduledExecutor.setProperties(toTypedProperties(evictionScheduledExecutorProperties));
   }

   public void setEvictionScheduledExecutorProperties(String evictionScheduledExecutorPropertiesString) {
      evictionScheduledExecutor.setProperties(toTypedProperties(evictionScheduledExecutorPropertiesString));
   }

   public Properties getReplicationQueueScheduledExecutorProperties() {
      return replicationQueueScheduledExecutor.properties;
   }

   public void setReplicationQueueScheduledExecutorProperties(Properties replicationQueueScheduledExecutorProperties) {
      this.replicationQueueScheduledExecutor.setProperties(toTypedProperties(replicationQueueScheduledExecutorProperties));
   }

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
   public void setMarshallVersion(String marshallVersion) {
      serialization.setVersion(marshallVersion);
   }

   public long getDistributedSyncTimeout() {
      return transport.distributedSyncTimeout;
   }

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
      
      @XmlElement(name = "properties")
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
   public static class ExecutorFactoryType extends FactoryClassWithPropertiesType {

      private static final long serialVersionUID = 6895901500645539386L;
            
      @XmlAttribute
      @ConfigurationDoc(name="factory", desc="Fully qualified class name of the ExecutorFactory to use.  Must implement org.infinispan.executors.ExecutorFactory")
      protected String factory = DefaultExecutorFactory.class.getName();

      public ExecutorFactoryType(String factory) {
         this.factory = factory;
      }

      public ExecutorFactoryType() {
      }

      public void setFactory(String factory) {
         testImmutability("factory");
         this.factory = factory;
      }

      @Override
      public ExecutorFactoryType clone() throws CloneNotSupportedException {
         return (ExecutorFactoryType) super.clone();
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
   public static class ScheduledExecutorFactoryType extends FactoryClassWithPropertiesType {

      private static final long serialVersionUID = 7806391452092647488L;
      
      @XmlAttribute
      @ConfigurationDoc(name="factory",desc="Fully qualified class name of the ScheduledExecutorFactory to use.  Must implement org.infinispan.executors.ScheduledExecutorFactory")
      protected String factory = DefaultScheduledExecutorFactory.class.getName();

      public ScheduledExecutorFactoryType(String factory) {
         this.factory = factory;
      }

      public ScheduledExecutorFactoryType() {
      }

      public void setFactory(String factory) {
         testImmutability("factory");
         this.factory = factory;
      }

      @Override
      public ScheduledExecutorFactoryType clone() throws CloneNotSupportedException {
         return (ScheduledExecutorFactoryType) super.clone();
      }
   }

   /**
    * This element configures the transport used for network communications across the cluster.
    * 
    * @see <a href="../../../config.html#ce_global_transport">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name="transport")
   public static class TransportType extends AbstractConfigurationBeanWithGCR {

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
      
      @ConfigurationDoc(name="distributedSyncTimeout",
                        desc="Infinispan uses a distributed lock to maintain a coherent transaction log during state transfer or rehashing, "
                              + "which means that only one cache can be doing state transfer or rehashing at the same time."
                              + "This constraint is in place because more than one cache could be involved in a transaction."
                              + "This timeout controls the time to wait to acquire acquire a lock on the distributed lock.")
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

      public String getMachineId() {
         return machineId;
      }

      public String getRackId() {
         return rackId;
      }

      public String getSiteId() {
         return siteId;
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

      @XmlAttribute
      public void setNodeName(String nodeName) {
         testImmutability("nodeName");
         this.nodeName = nodeName;
      }
      
      public void setProperties(TypedProperties properties) {
         testImmutability("properties");
         this.properties = properties;
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
   public static class SerializationType extends AbstractConfigurationBeanWithGCR {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = -925947118621507282L;

      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setMarshallerClass")
      protected String marshallerClass = VersionAwareMarshaller.class.getName(); // the default
      
      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setMarshallVersion")
      protected String version = Version.getMajorVersion();

      public SerializationType() {
         super();
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitSerializationType(this);
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
   }

   /**
    * This element specifies whether global statistics are gathered and reported via JMX for all
    * caches under this cache manager.
    * 
    * @see <a href="../../../config.html#ce_global_globalJmxStatistics">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   @ConfigurationDoc(name="globalJmxStatistics")
   public static class GlobalJmxStatisticsType extends AbstractConfigurationBeanWithGCR {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 6639689526822921024L;

      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setExposeGlobalJmxStatistics")
      protected Boolean enabled = false;
      
      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setJmxDomain")
      protected String jmxDomain = "org.infinispan";

      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setMBeanServerLookup")
      protected String mBeanServerLookup = PlatformMBeanServerLookup.class.getName();

      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setAllowDuplicateDomains")
      protected Boolean allowDuplicateDomains = false;

      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setCacheManagerName")
      protected String cacheManagerName = "DefaultCacheManager";

      @XmlElement(name = "properties")
      @ConfigurationDocRef(bean=GlobalConfiguration.class,targetElement="setMBeanServerProperties")
      protected TypedProperties properties = EMPTY_PROPERTIES;

      private MBeanServerLookup mBeanServerLookupInstance;

      @XmlAttribute
      public void setEnabled(Boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitGlobalJmxStatisticsType(this);
      }

       public void setProperties(TypedProperties p) {
         this.properties = p;
       }

      @XmlAttribute
      public void setJmxDomain(String jmxDomain) {
         testImmutability("jmxDomain");
         this.jmxDomain = jmxDomain;
      }

      @XmlAttribute
      public void setMBeanServerLookup(String beanServerLookup) {
         testImmutability("mBeanServerLookup");
         mBeanServerLookup = beanServerLookup;
      }

      @XmlAttribute
      public void setAllowDuplicateDomains(Boolean allowDuplicateDomains) {
         testImmutability("allowDuplicateDomains");
         this.allowDuplicateDomains = allowDuplicateDomains;
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
