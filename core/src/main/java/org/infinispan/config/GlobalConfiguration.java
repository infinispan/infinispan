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
import org.infinispan.jmx.PlatformMBeanServerLookup;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.TypedProperties;

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
 * A default instance of this bean takes default values for each attribute.  Please see the individual setters for
 * details of what these defaults are.
 * <p/>
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @configRef name="global",desc="Defines global settings shared among all cache instances created by a single
 * CacheManager."
 * @since 4.0
 * 
 * @see <a href="../../../config.html#ce_infinispan_global">Configuration reference</a>
 * 
 */

// Note that class GlobalConfiguration contains JAXB annotations. These annotations determine how XML configuration
// files are read into instances of configuration class hierarchy as well as they provide meta data for configuration
// file XML schema generation. Please modify these annotations and Java element types they annotate with utmost
// understanding and care.

@SurvivesRestarts
@Scope(Scopes.GLOBAL)
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder = {})
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
   private ExecutorFactoryType asyncListenerExecutor = new ExecutorFactoryType();

   @XmlElement
   private ExecutorFactoryType asyncTransportExecutor = new ExecutorFactoryType();

   @XmlElement
   private ScheduledExecutorFactoryType evictionScheduledExecutor = new ScheduledExecutorFactoryType();

   @XmlElement
   private ScheduledExecutorFactoryType replicationQueueScheduledExecutor = new ScheduledExecutorFactoryType();

   @XmlElement
   private GlobalJmxStatisticsType globalJmxStatistics = new GlobalJmxStatisticsType();

   @XmlElement
   private TransportType transport = new TransportType(null);

   @XmlElement
   private SerializationType serialization = new SerializationType();

   @XmlElement
   private ShutdownType shutdown = new ShutdownType();

   @XmlTransient
   private GlobalComponentRegistry gcr;

   public boolean isExposeGlobalJmxStatistics() {
      return globalJmxStatistics.enabled;
   }

   public void setExposeGlobalJmxStatistics(boolean exposeGlobalJmxStatistics) {
      testImmutability("exposeGlobalManagementStatistics");
      globalJmxStatistics.setEnabled(exposeGlobalJmxStatistics);
   }

   /**
    * If JMX statistics are enabled then all 'published' JMX objects will appear under this name. This is optional, if
    * not specified an object name will be created for you by default.
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

   public void setMBeanServerLookup(String mBeanServerLookup) {
      globalJmxStatistics.setMBeanServerLookup(mBeanServerLookup);
   }

   public boolean isAllowDuplicateDomains() {
      return globalJmxStatistics.allowDuplicateDomains;
   }

   public void setAllowDuplicateDomains(boolean allowDuplicateDomains) {
      globalJmxStatistics.setAllowDuplicateDomains(allowDuplicateDomains);
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

   public void setMarshallerClass(String marshallerClass) {
      serialization.setMarshallerClass(marshallerClass);
   }

   public String getTransportNodeName() {
      return transport.nodeName;
   }

   public void setTransportNodeName(String nodeName) {
      transport.setNodeName(nodeName);
   }

   public String getTransportClass() {
      return transport.transportClass;
   }


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

   public void setClusterName(String clusterName) {
      transport.setClusterName(clusterName);
   }

   public ShutdownHookBehavior getShutdownHookBehavior() {
      return shutdown.hookBehavior;
   }

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

   public void setMarshallVersion(short marshallVersion) {
      testImmutability("marshallVersion");
      serialization.version = Version.decodeVersionForSerialization(marshallVersion);
   }

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

      /**
       * @configPropertyRef name="maxThreads",desc="Maximum number of threads for this executor. Default values can be found <a href=&quot;http://community.jboss.org/docs/DOC-15540&quot;>here</a>"
       * @configPropertyRef name="threadNamePrefix",desc="Thread name prefix for threads created by this executor. Default values can be found <a href=&quot;http://community.jboss.org/docs/DOC-15540&quot;>here</a>"
       */
      @XmlElement(name = "properties")
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
    * @configRef name="asyncListenerExecutor",desc="Configuration for the executor service used to emit notifications to
    * asynchronous listeners."
    * @configRef name="asyncTransportExecutor",desc="Configuration for the executor service used for asynchronous work
    * on the Transport, including asynchronous marshalling and Cache 'async operations' such as Cache.putAsync()."
    * 
    * @see <a href="../../../config.html#ce_global_asyncListenerExecutor">Configuration reference</a>
    * 
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class ExecutorFactoryType extends FactoryClassWithPropertiesType {

      private static final long serialVersionUID = 6895901500645539386L;
      
      /**
       * @configRef desc="Fully qualified class name of the ExecutorFactory to use.  Must
       * implement org.infinispan.executors.ExecutorFactory"
       */
      @XmlAttribute
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
    * @configRef name="evictionScheduledExecutor",desc="Configuration for the scheduled executor service used to
    * periodically run eviction cleanup tasks."
    * @configRef name="replicationQueueScheduledExecutor",desc="Configuration for the scheduled executor service used to
    * periodically flush replication queues, used if asynchronous clustering is enabled along with useReplQueue being
    * set to true."
    * 
    * @see <a href="../../../config.html#ce_global_evictionScheduledExecutor">Configuration reference</a>
    * 
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class ScheduledExecutorFactoryType extends FactoryClassWithPropertiesType {

      private static final long serialVersionUID = 7806391452092647488L;
      
      /**
       * @configRef desc="Fully qualified class name of the ScheduledExecutorFactory to use.  Must
       * implement org.infinispan.executors.ScheduledExecutorFactory"
       */
      @XmlAttribute
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
    * @configRef name="transport",desc="This element configures the transport used for network communications across the
    * cluster."
    * 
    * @see <a href="../../../config.html#ce_global_transport">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class TransportType extends AbstractConfigurationBeanWithGCR {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = -4739815717370060368L;

      /**
       * @configRef desc="This defines the name of the cluster.  Nodes only connect to clusters sharing the same name."
       */
      protected String clusterName = "Infinispan-Cluster";

      /**
       * @configRef desc="Cluster-wide synchronization timeout for locks.  Used to coordinate changes in cluster
       * membership."
       */
      protected Long distributedSyncTimeout = 60000L; // default

      /**
       * @configRef desc="Fully qualified name of a class that represents a network transport.  Must implement
       * org.infinispan.remoting.transport.Transport"
       */
      protected String transportClass = null; // The default constructor sets default to JGroupsTransport

      /**
       * @configRef desc="Name of the current node.  This is a friendly name to make logs, etc. make more sense.
       * Defaults to a combination of host name and a random number (to differentiate multiple nodes on the same host)"
       */
      protected String nodeName = null;

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

      @XmlElement
      public void setProperties(TypedProperties properties) {
         testImmutability("properties");
         this.properties = properties;
      }

      @Override
      public TransportType clone() throws CloneNotSupportedException {
         TransportType dolly = (TransportType) super.clone();
         dolly.properties = (TypedProperties) properties.clone();
         return dolly;
      }
   }

   /**
    * @configRef name="serialization",desc="Serialization and marshalling settings."
    * 
    * @see <a href="../../../config.html#ce_global_serialization">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class SerializationType extends AbstractConfigurationBeanWithGCR {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = -925947118621507282L;

      /**
       * @configRef desc="Fully qualified name of the marshaller to use. It must implement
       * org.infinispan.marshall.StreamingMarshaller."
       */
      protected String marshallerClass = VersionAwareMarshaller.class.getName(); // the default

      /**
       * @configRef desc="Largest allowable version to use when marshalling internal state.  Set this to the lowest
       * version cache instance in your cluster to ensure compatibility of communications.  However, setting this too
       * low will mean you lose out on the benefit of improvements in newer versions of the marshaller."
       */
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
    * @configRef name="globalJmxStatistics",desc="This element specifies whether global statistics are gathered and
    * reported via JMX for all caches under this cache manager."
    * 
    * @see <a href="../../../config.html#ce_global_globalJmxStatistics">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class GlobalJmxStatisticsType extends AbstractConfigurationBeanWithGCR {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 6639689526822921024L;

      /**
       * @configRef desc="Toggle to enable/disable global statistics being exported via JMX."
       */
      protected Boolean enabled = false;

      /**
       * @configRef desc="JMX domain name where all relevant JMX exposed objects will be bound"
       */
      protected String jmxDomain = "infinispan";

      /**
       * @configRef desc="Fully qualified name of class that will attempt to locate a JMX MBean server to bind to"
       */
      protected String mBeanServerLookup = PlatformMBeanServerLookup.class.getName();

      /**
       * @configRef desc="If true, multiple cache manager instances could be configured under the same configured JMX
       * domain. Each cache manager will in practice use a different JMX domain that has been calculated based on the
       * configured one by adding an incrementing index to it."
       */
      protected Boolean allowDuplicateDomains = false;

      @XmlAttribute
      public void setEnabled(Boolean enabled) {
         testImmutability("enabled");
         this.enabled = enabled;
      }

      public void accept(ConfigurationBeanVisitor v) {
         v.visitGlobalJmxStatisticsType(this);
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
   }

   /**
    * @configRef name="shutdown",desc=" This element specifies behavior when the JVM running the cache instance shuts
    * down."
    * 
    * @see <a href="../../../config.html#ce_global_shutdown">Configuration reference</a>
    */
   @XmlAccessorType(XmlAccessType.PROPERTY)
   public static class ShutdownType extends AbstractConfigurationBeanWithGCR {

      /**
       * The serialVersionUID
       */
      private static final long serialVersionUID = 3427920991221031456L;

      /**
       * @configRef desc="Behavior of the JVM shutdown hook registered by the cache. The options available are: DEFAULT
       * - A shutdown hook is registered even if no MBean server (apart from the JDK default) is detected. REGISTER -
       * Forces the cache to register a shutdown hook even if an MBean server is detected. DONT_REGISTER - Forces the
       * cache NOT to register a shutdown hook, even if no MBean server is detected.
       */
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
