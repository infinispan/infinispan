package org.infinispan.config;

import org.infinispan.CacheException;
import org.infinispan.Version;
import org.infinispan.executors.DefaultExecutorFactory;
import org.infinispan.executors.DefaultScheduledExecutorFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.NonVolatile;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.PlatformMBeanServerLookup;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.VersionAwareMarshaller;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.util.TypedProperties;

import java.util.Properties;

/**
 * Configuration component that encapsulates the global configuration.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@NonVolatile
@Scope(Scopes.GLOBAL)
@ConfigurationElements(elements = {
         @ConfigurationElement(name = "global", parent = "infinispan", description = ""),
         @ConfigurationElement(name = "asyncListenerExecutor", parent = "global", description = ""), 
         @ConfigurationElement(name = "transport", parent = "global", description = ""), 
         @ConfigurationElement(name = "evictionScheduledExecutor", parent = "global", description = ""),
         @ConfigurationElement(name = "replicationQueueScheduledExecutor", parent = "global", description = ""),  
         @ConfigurationElement(name = "globalJmxStatistics", parent = "global", description = ""),   
         @ConfigurationElement(name = "asyncTransportExecutor", parent = "global", description = ""),
         @ConfigurationElement(name = "serialization", parent = "global", description = ""),
         @ConfigurationElement(name = "shutdown", parent = "global", description = "")
})
public class GlobalConfiguration extends AbstractConfigurationBean {

   /**
    * Default replication version, from {@link org.infinispan.Version#getVersionShort}.
    */
   public static final short DEFAULT_MARSHALL_VERSION = Version.getVersionShort();

   private String asyncListenerExecutorFactoryClass = DefaultExecutorFactory.class.getName();
   private TypedProperties asyncListenerExecutorProperties = EMPTY_PROPERTIES;
   private String asyncTransportExecutorFactoryClass = DefaultExecutorFactory.class.getName();
   private TypedProperties asyncTransportExecutorProperties = EMPTY_PROPERTIES;
   private String evictionScheduledExecutorFactoryClass = DefaultScheduledExecutorFactory.class.getName();
   private TypedProperties evictionScheduledExecutorProperties = EMPTY_PROPERTIES;
   private String replicationQueueScheduledExecutorFactoryClass = DefaultScheduledExecutorFactory.class.getName();
   private TypedProperties replicationQueueScheduledExecutorProperties = EMPTY_PROPERTIES;
   private String marshallerClass = VersionAwareMarshaller.class.getName(); // the default
   private String transportClass = null; // this defaults to a non-clustered cache.
   private TypedProperties transportProperties = EMPTY_PROPERTIES;
   private Configuration defaultConfiguration;
   private String clusterName = "Infinispan-Cluster";
   private ShutdownHookBehavior shutdownHookBehavior = ShutdownHookBehavior.DEFAULT;
   private short marshallVersion = DEFAULT_MARSHALL_VERSION;

   private GlobalComponentRegistry gcr;
   private long distributedSyncTimeout = 60000; // default

   private boolean exposeGlobalJmxStatistics = false;
   private String jmxDomain = "infinispan";
   private String mBeanServerLookup = PlatformMBeanServerLookup.class.getName();
   private boolean allowDuplicateDomains = false;

   public boolean isExposeGlobalJmxStatistics() {
      return exposeGlobalJmxStatistics;
   }

   @ConfigurationAttribute(name = "enabled", 
            containingElement = "globalJmxStatistics", 
            description = "If true, global JMX statistics are published")
   public void setExposeGlobalJmxStatistics(boolean exposeGlobalJmxStatistics) {
      testImmutability("exposeGlobalManagementStatistics");
      this.exposeGlobalJmxStatistics = exposeGlobalJmxStatistics;
   }

   /**
    * If JMX statistics are enabled then all 'published' JMX objects will appear under this name. This is optional, if
    * not specified an object name will be created for you by default.
    */
   @ConfigurationAttribute(name = "jmxDomain", 
            containingElement = "globalJmxStatistics", 
            description = "If JMX statistics are enabled then all 'published' JMX objects will appear under this name")
   public void setJmxDomain(String jmxObjectName) {
      testImmutability("jmxNameBase");
      this.jmxDomain = jmxObjectName;
   }

   /**
    * @see #setJmxDomain(String)
    */
   public String getJmxDomain() {
      return jmxDomain;
   }

   public String getMBeanServerLookup() {
      return mBeanServerLookup;
   }

   @ConfigurationAttribute(name = "mBeanServerLookup", 
            containingElement = "globalJmxStatistics", 
            description = "")
   public void setMBeanServerLookup(String mBeanServerLookup) {
      testImmutability("mBeanServerLookup");
      this.mBeanServerLookup = mBeanServerLookup;
   }

   public boolean isAllowDuplicateDomains() {
      return allowDuplicateDomains;
   }

   @ConfigurationAttribute(name = "allowDuplicateDomains", 
            containingElement = "globalJmxStatistics", 
            description = "")
   public void setAllowDuplicateDomains(boolean allowDuplicateDomains) {
      testImmutability("allowDuplicateDomains");
      this.allowDuplicateDomains = allowDuplicateDomains;
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
   }

   protected boolean hasComponentStarted() {
      return gcr != null && gcr.getStatus() != null && gcr.getStatus() == ComponentStatus.RUNNING;
   }

   public String getAsyncListenerExecutorFactoryClass() {
      return asyncListenerExecutorFactoryClass;
   }

   @ConfigurationAttribute(name = "factory", 
            containingElement = "asyncListenerExecutor", 
            description = "ExecutorService factory class for asynchronous listeners")
   public void setAsyncListenerExecutorFactoryClass(String asyncListenerExecutorFactoryClass) {
      testImmutability("asyncListenerExecutorFactoryClass");
      this.asyncListenerExecutorFactoryClass = asyncListenerExecutorFactoryClass;
   }

   public String getAsyncTransportExecutorFactoryClass() {
      return asyncTransportExecutorFactoryClass;
   }

   @ConfigurationAttribute(name = "factory", 
            containingElement = "asyncTransportExecutor", 
            description = "ExecutorService factory class for async transport")
   public void setAsyncTransportExecutorFactoryClass(String asyncTransportExecutorFactoryClass) {
      testImmutability("asyncTransportExecutorFactoryClass");
      this.asyncTransportExecutorFactoryClass = asyncTransportExecutorFactoryClass;
   }

   public String getEvictionScheduledExecutorFactoryClass() {
      return evictionScheduledExecutorFactoryClass;
   }

   @ConfigurationAttribute(name = "factory", 
            containingElement = "evictionScheduledExecutor", 
            description = "ExecutorService factory class for eviction threads")
   public void setEvictionScheduledExecutorFactoryClass(String evictionScheduledExecutorFactoryClass) {
      testImmutability("evictionScheduledExecutorFactoryClass");
      this.evictionScheduledExecutorFactoryClass = evictionScheduledExecutorFactoryClass;
   }

   public String getReplicationQueueScheduledExecutorFactoryClass() {
      return replicationQueueScheduledExecutorFactoryClass;
   }

   @ConfigurationAttribute(name = "factory", 
            containingElement = "replicationQueueScheduledExecutor", 
            description = "ExecutorService factory class for replication queue threads")
   public void setReplicationQueueScheduledExecutorFactoryClass(String replicationQueueScheduledExecutorFactoryClass) {
      testImmutability("replicationQueueScheduledExecutorFactoryClass");
      this.replicationQueueScheduledExecutorFactoryClass = replicationQueueScheduledExecutorFactoryClass;
   }

   public String getMarshallerClass() {
      return marshallerClass;
   }

   @ConfigurationAttribute(name = "marshallerClass", 
            containingElement = "serialization")
   public void setMarshallerClass(String marshallerClass) {
      testImmutability("marshallerClass");
      this.marshallerClass = marshallerClass;
   }

   public String getTransportClass() {
      return transportClass;
   }

   @ConfigurationAttribute(name = "transportClass", 
            containingElement = "transport", 
            description = "Transport class, by default null i.e. no transport",
            defaultValue = "org.infinispan.remoting.transport.jgroups.JGroupsTransport")
   public void setTransportClass(String transportClass) {
      testImmutability("transportClass");
      this.transportClass = transportClass;
   }

   public Properties getTransportProperties() {
      return transportProperties;
   }

   public void setTransportProperties(Properties transportProperties) {
      testImmutability("transportProperties");
      this.transportProperties = toTypedProperties(transportProperties);
   }

   @ConfigurationProperties(elements = {
            @ConfigurationProperty(name = "configurationString", parentElement = "transport"),
            @ConfigurationProperty(name = "configurationFile", parentElement = "transport"),
            @ConfigurationProperty(name = "configurationXml", parentElement = "transport") })
   public void setTransportProperties(String transportPropertiesString) {
      testImmutability("transportProperties");
      this.transportProperties = toTypedProperties(transportPropertiesString);
   }

   public Configuration getDefaultConfiguration() {
      return defaultConfiguration;
   }

   public void setDefaultConfiguration(Configuration defaultConfiguration) {
      testImmutability("defaultConfiguration");
      this.defaultConfiguration = defaultConfiguration;
   }

   public String getClusterName() {
      return clusterName;
   }

   @ConfigurationAttribute(name = "clusterName", 
            containingElement = "transport")
   public void setClusterName(String clusterName) {
      testImmutability("clusterName");
      this.clusterName = clusterName;
   }

   public ShutdownHookBehavior getShutdownHookBehavior() {
      return shutdownHookBehavior;
   }

   public void setShutdownHookBehavior(ShutdownHookBehavior shutdownHookBehavior) {
      testImmutability("shutdownHookBehavior");
      this.shutdownHookBehavior = shutdownHookBehavior;
   }

   @ConfigurationAttribute(name = "hookBehavior", 
            containingElement = "shutdown", 
            allowedValues = "DEFAULT, REGISTER, DONT_REGISTER", 
            defaultValue = "DEFAULT")
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
      return asyncListenerExecutorProperties;
   }
   

   @ConfigurationProperties(elements = {
            @ConfigurationProperty(name = "maxThreads", parentElement = "asyncListenerExecutor"),
            @ConfigurationProperty(name = "threadNamePrefix", parentElement = "asyncListenerExecutor") })
   public void setAsyncListenerExecutorProperties(Properties asyncListenerExecutorProperties) {
      testImmutability("asyncListenerExecutorProperties");
      this.asyncListenerExecutorProperties = toTypedProperties(asyncListenerExecutorProperties);
   }

   public void setAsyncListenerExecutorProperties(String asyncListenerExecutorPropertiesString) {
      testImmutability("asyncListenerExecutorProperties");
      this.asyncListenerExecutorProperties = toTypedProperties(asyncListenerExecutorPropertiesString);
   }

   public Properties getAsyncTransportExecutorProperties() {
      return asyncTransportExecutorProperties;
   }

   @ConfigurationProperties(elements = {
            @ConfigurationProperty(name = "maxThreads", parentElement = "asyncTransportExecutor"),
            @ConfigurationProperty(name = "threadNamePrefix", parentElement = "asyncTransportExecutor") })
   public void setAsyncTransportExecutorProperties(Properties asyncTransportExecutorProperties) {
      testImmutability("asyncTransportExecutorProperties");
      this.asyncTransportExecutorProperties = toTypedProperties(asyncTransportExecutorProperties);
   }

   public void setAsyncTransportExecutorProperties(String asyncSerializationExecutorPropertiesString) {
      testImmutability("asyncTransportExecutorProperties");
      this.asyncTransportExecutorProperties = toTypedProperties(asyncSerializationExecutorPropertiesString);
   }

   public Properties getEvictionScheduledExecutorProperties() {
      return evictionScheduledExecutorProperties;
   }


   @ConfigurationProperties(elements = {
            @ConfigurationProperty(name = "maxThreads", parentElement = "evictionScheduledExecutor"),
            @ConfigurationProperty(name = "threadNamePrefix", parentElement = "evictionScheduledExecutor") })   
   public void setEvictionScheduledExecutorProperties(Properties evictionScheduledExecutorProperties) {
      testImmutability("evictionScheduledExecutorProperties");
      this.evictionScheduledExecutorProperties = toTypedProperties(evictionScheduledExecutorProperties);
   }

   public void setEvictionScheduledExecutorProperties(String evictionScheduledExecutorPropertiesString) {
      testImmutability("evictionScheduledExecutorProperties");
      this.evictionScheduledExecutorProperties = toTypedProperties(evictionScheduledExecutorPropertiesString);
   }

   public Properties getReplicationQueueScheduledExecutorProperties() {
      return replicationQueueScheduledExecutorProperties;
   }

   @ConfigurationProperties(elements = {
            @ConfigurationProperty(name = "maxThreads", parentElement = "replicationQueueScheduledExecutor"),
            @ConfigurationProperty(name = "threadNamePrefix", parentElement = "replicationQueueScheduledExecutor") })      
   public void setReplicationQueueScheduledExecutorProperties(Properties replicationQueueScheduledExecutorProperties) {
      testImmutability("replicationQueueScheduledExecutorProperties");
      this.replicationQueueScheduledExecutorProperties = toTypedProperties(replicationQueueScheduledExecutorProperties);
   }

   public void setReplicationQueueScheduledExecutorProperties(String replicationQueueScheduledExecutorPropertiesString) {
      testImmutability("replicationQueueScheduledExecutorProperties");
      this.replicationQueueScheduledExecutorProperties = toTypedProperties(replicationQueueScheduledExecutorPropertiesString);
   }

   public short getMarshallVersion() {
      return marshallVersion;
   }

   public String getMarshallVersionString() {
      return Version.decodeVersionForSerialization(marshallVersion);
   }

   public void setMarshallVersion(short marshallVersion) {
      testImmutability("marshallVersion");
      this.marshallVersion = marshallVersion;
   }
   
   @ConfigurationAttribute(name = "version", 
            containingElement = "serialization")
   public void setMarshallVersion(String marshallVersion) {
      testImmutability("marshallVersion");
      this.marshallVersion = Version.getVersionShort(marshallVersion);
   }

   public long getDistributedSyncTimeout() {
      return distributedSyncTimeout;
   }
   
   @ConfigurationAttribute(name = "distributedSyncTimeout", 
            containingElement = "transport")
   public void setDistributedSyncTimeout(long distributedSyncTimeout) {
      testImmutability("distributedSyncTimeout");
      this.distributedSyncTimeout = distributedSyncTimeout;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      GlobalConfiguration that = (GlobalConfiguration) o;

      if (marshallVersion != that.marshallVersion) return false;
      if (asyncListenerExecutorFactoryClass != null ? !asyncListenerExecutorFactoryClass.equals(that.asyncListenerExecutorFactoryClass) : that.asyncListenerExecutorFactoryClass != null)
         return false;
      if (asyncListenerExecutorProperties != null ? !asyncListenerExecutorProperties.equals(that.asyncListenerExecutorProperties) : that.asyncListenerExecutorProperties != null)
         return false;
      if (asyncTransportExecutorFactoryClass != null ? !asyncTransportExecutorFactoryClass.equals(that.asyncTransportExecutorFactoryClass) : that.asyncTransportExecutorFactoryClass != null)
         return false;
      if (asyncTransportExecutorProperties != null ? !asyncTransportExecutorProperties.equals(that.asyncTransportExecutorProperties) : that.asyncTransportExecutorProperties != null)
         return false;
      if (clusterName != null ? !clusterName.equals(that.clusterName) : that.clusterName != null) return false;
      if (defaultConfiguration != null ? !defaultConfiguration.equals(that.defaultConfiguration) : that.defaultConfiguration != null)
         return false;
      if (evictionScheduledExecutorFactoryClass != null ? !evictionScheduledExecutorFactoryClass.equals(that.evictionScheduledExecutorFactoryClass) : that.evictionScheduledExecutorFactoryClass != null)
         return false;
      if (evictionScheduledExecutorProperties != null ? !evictionScheduledExecutorProperties.equals(that.evictionScheduledExecutorProperties) : that.evictionScheduledExecutorProperties != null)
         return false;
      if (marshallerClass != null ? !marshallerClass.equals(that.marshallerClass) : that.marshallerClass != null)
         return false;
      if (replicationQueueScheduledExecutorFactoryClass != null ? !replicationQueueScheduledExecutorFactoryClass.equals(that.replicationQueueScheduledExecutorFactoryClass) : that.replicationQueueScheduledExecutorFactoryClass != null)
         return false;
      if (replicationQueueScheduledExecutorProperties != null ? !replicationQueueScheduledExecutorProperties.equals(that.replicationQueueScheduledExecutorProperties) : that.replicationQueueScheduledExecutorProperties != null)
         return false;
      if (shutdownHookBehavior != that.shutdownHookBehavior) return false;
      if (transportClass != null ? !transportClass.equals(that.transportClass) : that.transportClass != null)
         return false;
      if (transportProperties != null ? !transportProperties.equals(that.transportProperties) : that.transportProperties != null)
         return false;
      if (distributedSyncTimeout != that.distributedSyncTimeout) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = asyncListenerExecutorFactoryClass != null ? asyncListenerExecutorFactoryClass.hashCode() : 0;
      result = 31 * result + (asyncListenerExecutorProperties != null ? asyncListenerExecutorProperties.hashCode() : 0);
      result = 31 * result + (asyncTransportExecutorFactoryClass != null ? asyncTransportExecutorFactoryClass.hashCode() : 0);
      result = 31 * result + (asyncTransportExecutorProperties != null ? asyncTransportExecutorProperties.hashCode() : 0);
      result = 31 * result + (evictionScheduledExecutorFactoryClass != null ? evictionScheduledExecutorFactoryClass.hashCode() : 0);
      result = 31 * result + (evictionScheduledExecutorProperties != null ? evictionScheduledExecutorProperties.hashCode() : 0);
      result = 31 * result + (replicationQueueScheduledExecutorFactoryClass != null ? replicationQueueScheduledExecutorFactoryClass.hashCode() : 0);
      result = 31 * result + (replicationQueueScheduledExecutorProperties != null ? replicationQueueScheduledExecutorProperties.hashCode() : 0);
      result = 31 * result + (marshallerClass != null ? marshallerClass.hashCode() : 0);
      result = 31 * result + (transportClass != null ? transportClass.hashCode() : 0);
      result = 31 * result + (transportProperties != null ? transportProperties.hashCode() : 0);
      result = 31 * result + (defaultConfiguration != null ? defaultConfiguration.hashCode() : 0);
      result = 31 * result + (clusterName != null ? clusterName.hashCode() : 0);
      result = 31 * result + (shutdownHookBehavior != null ? shutdownHookBehavior.hashCode() : 0);
      result = 31 * result + (int) marshallVersion;
      result = (int) (31 * result + distributedSyncTimeout);
      return result;
   }

   @Override
   public GlobalConfiguration clone() {
      try {
         return (GlobalConfiguration) super.clone();
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
}
