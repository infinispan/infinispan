package org.infinispan.config;

import org.infinispan.CacheException;
import org.infinispan.Version;
import org.infinispan.jmx.PlatformMBeanServerLookup;
import org.infinispan.executors.DefaultExecutorFactory;
import org.infinispan.executors.DefaultScheduledExecutorFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.NonVolatile;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
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
public class GlobalConfiguration extends AbstractConfigurationBean {

   /**
    * Default replication version, from {@link org.infinispan.Version#getVersionShort}.
    */
   public static final short DEFAULT_MARSHALL_VERSION = Version.getVersionShort();

   private String asyncListenerExecutorFactoryClass = DefaultExecutorFactory.class.getName();
   private TypedProperties asyncListenerExecutorProperties = EMPTY_PROPERTIES;
   private String asyncSerializationExecutorFactoryClass = DefaultExecutorFactory.class.getName();
   private TypedProperties asyncSerializationExecutorProperties = EMPTY_PROPERTIES;
   private String evictionScheduledExecutorFactoryClass = DefaultScheduledExecutorFactory.class.getName();
   private TypedProperties evictionScheduledExecutorProperties = EMPTY_PROPERTIES;
   private String replicationQueueScheduledExecutorFactoryClass = DefaultScheduledExecutorFactory.class.getName();
   private TypedProperties replicationQueueScheduledExecutorProperties = EMPTY_PROPERTIES;
   private String marshallerClass = VersionAwareMarshaller.class.getName(); // the default
   private int objectInputStreamPoolSize = 50; // defaults
   private int objectOutputStreamPoolSize = 50; // defaults
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

   public void setExposeGlobalJmxStatistics(boolean exposeGlobalJmxStatistics) {
      testImmutability("exposeGlobalManagementStatistics");
      this.exposeGlobalJmxStatistics = exposeGlobalJmxStatistics;
   }

   /**
    * If JMX statistics are enabled then all 'published' JMX objects will appear under this name. This is optional, if
    * not specified an object name will be created for you by default.
    *
    * @see javax.management.ObjectName
    * @see #isExposeManagementStatistics()
    */
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

   public void setMBeanServerLookup(String mBeanServerLookup) {
      testImmutability("mBeanServerLookup");
      this.mBeanServerLookup = mBeanServerLookup;
   }

   public boolean isAllowDuplicateDomains() {
      return allowDuplicateDomains;
   }

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

   public void setAsyncListenerExecutorFactoryClass(String asyncListenerExecutorFactoryClass) {
      testImmutability("asyncListenerExecutorFactoryClass");
      this.asyncListenerExecutorFactoryClass = asyncListenerExecutorFactoryClass;
   }

   public String getAsyncSerializationExecutorFactoryClass() {
      return asyncSerializationExecutorFactoryClass;
   }

   public void setAsyncSerializationExecutorFactoryClass(String asyncSerializationExecutorFactoryClass) {
      testImmutability("asyncSerializationExecutorFactoryClass");
      this.asyncSerializationExecutorFactoryClass = asyncSerializationExecutorFactoryClass;
   }

   public String getEvictionScheduledExecutorFactoryClass() {
      return evictionScheduledExecutorFactoryClass;
   }

   public void setEvictionScheduledExecutorFactoryClass(String evictionScheduledExecutorFactoryClass) {
      testImmutability("evictionScheduledExecutorFactoryClass");
      this.evictionScheduledExecutorFactoryClass = evictionScheduledExecutorFactoryClass;
   }

   public String getReplicationQueueScheduledExecutorFactoryClass() {
      return replicationQueueScheduledExecutorFactoryClass;
   }

   public void setReplicationQueueScheduledExecutorFactoryClass(String replicationQueueScheduledExecutorFactoryClass) {
      testImmutability("replicationQueueScheduledExecutorFactoryClass");
      this.replicationQueueScheduledExecutorFactoryClass = replicationQueueScheduledExecutorFactoryClass;
   }

   public String getMarshallerClass() {
      return marshallerClass;
   }

   public void setMarshallerClass(String marshallerClass) {
      testImmutability("marshallerClass");
      this.marshallerClass = marshallerClass;
   }

   public int getObjectInputStreamPoolSize() {
      return objectInputStreamPoolSize;
   }

   public void setObjectInputStreamPoolSize(int objectInputStreamPoolSize) {
      testImmutability("objectInputStreamPoolSize");
      this.objectInputStreamPoolSize = objectInputStreamPoolSize;
   }

   public int getObjectOutputStreamPoolSize() {
      return objectOutputStreamPoolSize;
   }

   public void setObjectOutputStreamPoolSize(int objectOutputStreamPoolSize) {
      testImmutability("objectOutputStreamPoolSize");
      this.objectOutputStreamPoolSize = objectOutputStreamPoolSize;
   }

   public String getTransportClass() {
      return transportClass;
   }

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

   public void setAsyncListenerExecutorProperties(Properties asyncListenerExecutorProperties) {
      testImmutability("asyncListenerExecutorProperties");
      this.asyncListenerExecutorProperties = toTypedProperties(asyncListenerExecutorProperties);
   }

   public void setAsyncListenerExecutorProperties(String asyncListenerExecutorPropertiesString) {
      testImmutability("asyncListenerExecutorProperties");
      this.asyncListenerExecutorProperties = toTypedProperties(asyncListenerExecutorPropertiesString);
   }

   public Properties getAsyncSerializationExecutorProperties() {
      return asyncSerializationExecutorProperties;
   }

   public void setAsyncSerializationExecutorProperties(Properties asyncSerializationExecutorProperties) {
      testImmutability("asyncSerializationExecutorProperties");
      this.asyncSerializationExecutorProperties = toTypedProperties(asyncSerializationExecutorProperties);
   }

   public void setAsyncSerializationExecutorProperties(String asyncSerializationExecutorPropertiesString) {
      testImmutability("asyncSerializationExecutorProperties");
      this.asyncSerializationExecutorProperties = toTypedProperties(asyncSerializationExecutorPropertiesString);
   }

   public Properties getEvictionScheduledExecutorProperties() {
      return evictionScheduledExecutorProperties;
   }

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

   public void setMarshallVersion(String marshallVersion) {
      testImmutability("marshallVersion");
      this.marshallVersion = Version.getVersionShort(marshallVersion);
   }

   public long getDistributedSyncTimeout() {
      return distributedSyncTimeout;
   }

   public void setDistributedSyncTimeout(long distributedSyncTimeout) {
      testImmutability("distributedSyncTimeout");
      this.distributedSyncTimeout = distributedSyncTimeout;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      GlobalConfiguration that = (GlobalConfiguration) o;

      if (objectInputStreamPoolSize != that.objectInputStreamPoolSize) return false;
      if (objectOutputStreamPoolSize != that.objectOutputStreamPoolSize) return false;
      if (marshallVersion != that.marshallVersion) return false;
      if (asyncListenerExecutorFactoryClass != null ? !asyncListenerExecutorFactoryClass.equals(that.asyncListenerExecutorFactoryClass) : that.asyncListenerExecutorFactoryClass != null)
         return false;
      if (asyncListenerExecutorProperties != null ? !asyncListenerExecutorProperties.equals(that.asyncListenerExecutorProperties) : that.asyncListenerExecutorProperties != null)
         return false;
      if (asyncSerializationExecutorFactoryClass != null ? !asyncSerializationExecutorFactoryClass.equals(that.asyncSerializationExecutorFactoryClass) : that.asyncSerializationExecutorFactoryClass != null)
         return false;
      if (asyncSerializationExecutorProperties != null ? !asyncSerializationExecutorProperties.equals(that.asyncSerializationExecutorProperties) : that.asyncSerializationExecutorProperties != null)
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
      result = 31 * result + (asyncSerializationExecutorFactoryClass != null ? asyncSerializationExecutorFactoryClass.hashCode() : 0);
      result = 31 * result + (asyncSerializationExecutorProperties != null ? asyncSerializationExecutorProperties.hashCode() : 0);
      result = 31 * result + (evictionScheduledExecutorFactoryClass != null ? evictionScheduledExecutorFactoryClass.hashCode() : 0);
      result = 31 * result + (evictionScheduledExecutorProperties != null ? evictionScheduledExecutorProperties.hashCode() : 0);
      result = 31 * result + (replicationQueueScheduledExecutorFactoryClass != null ? replicationQueueScheduledExecutorFactoryClass.hashCode() : 0);
      result = 31 * result + (replicationQueueScheduledExecutorProperties != null ? replicationQueueScheduledExecutorProperties.hashCode() : 0);
      result = 31 * result + (marshallerClass != null ? marshallerClass.hashCode() : 0);
      result = 31 * result + objectInputStreamPoolSize;
      result = 31 * result + objectOutputStreamPoolSize;
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
