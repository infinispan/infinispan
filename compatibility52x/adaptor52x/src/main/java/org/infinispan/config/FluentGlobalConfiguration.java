
package org.infinispan.config;

import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.MBeanServerLookup;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.marshall.AdvancedExternalizer;
import org.infinispan.marshall.Marshaller;
import org.infinispan.remoting.transport.Transport;
import org.jboss.marshalling.ClassResolver;

import java.util.Properties;

/**
 * Fluent global configuration base class.
 *
 * @author Galder Zamarreño
 * @author Vladimir Blagojevic
 * @since 5.0
 */
@Deprecated
public class FluentGlobalConfiguration extends AbstractConfigurationBeanWithGCR {

   public FluentGlobalConfiguration(GlobalConfiguration globalConfig) {
      setGlobalConfiguration(globalConfig);
   }

   /**
    * Configures serialization and marshalling settings.
    */
   @Deprecated public static interface SerializationConfig extends FluentGlobalTypes {
      /**
       * Fully qualified name of the marshaller to use. It must implement
       * org.infinispan.marshall.StreamingMarshaller
       *
       * @param marshallerClass
       */
      SerializationConfig marshallerClass(Class<? extends Marshaller> marshallerClass);

      /**
       * Largest allowable version to use when marshalling internal state. Set this to the lowest
       * version cache instance in your cluster to ensure compatibility of communications. However,
       * setting this too low will mean you lose out on the benefit of improvements in newer
       * versions of the marshaller.
       *
       * @param marshallVersion
       */
      SerializationConfig version(String marshallVersion);

      SerializationConfig version(short marshallVersion);

      /**
       * Adds an {@link org.infinispan.marshall.AdvancedExternalizer} with the give id.
       *
       * @param <T>     type of the object that the AdvancedExternalizer marshalls
       * @param clazz   externalizer class
       * @param id      id of externlizer
       * @return this   ExternalizersConfig
       */
      <T> SerializationConfig addAdvancedExternalizer(int id, Class<? extends AdvancedExternalizer<T>> clazz);

      /**
       * Adds an {@link org.infinispan.marshall.AdvancedExternalizer}.
       *
       * @param <T>     type of the object that the AdvancedExternalizer marshalls
       * @param clazz   externalizer class
       * @return this   ExternalizersConfig
       */
      <T> SerializationConfig addAdvancedExternalizer(Class<? extends AdvancedExternalizer<T>> clazz);

      /**
       * Helper method that allows for quick registration of an {@link org.infinispan.marshall.AdvancedExternalizer} implementation
       * alongside its corresponding identifier. Remember that the identifier needs to a be positive
       * number, including 0, and cannot clash with other identifiers in the system.
       *
       * @param id
       * @param advancedExternalizer
       */
      <T> SerializationConfig addAdvancedExternalizer(int id, AdvancedExternalizer<T> advancedExternalizer);

      /**
       * Helper method that allows for quick registration of {@link org.infinispan.marshall.AdvancedExternalizer} implementations.
       *
       * @param advancedExternalizers
       */
      <T> SerializationConfig addAdvancedExternalizer(AdvancedExternalizer<T>... advancedExternalizers);

      /**
       * Class resolver to use when unmarshallig objects.
       *
       * @param classResolver
       */
      SerializationConfig classResolver(ClassResolver classResolver);
   }

   /**
    * Configures the transport used for network communications across the cluster.
    */
   @Deprecated public static interface TransportConfig extends FluentGlobalTypes {
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
       * @param strictPeerToPeer flag controlling this behavior
       */
      TransportConfig strictPeerToPeer(Boolean strictPeerToPeer);

      TransportConfig addProperty(String key, String value);
   }

   /**
    * Configures whether global statistics are gathered and reported via JMX for all caches under this cache manager.
    */
   @Deprecated public static interface GlobalJmxStatisticsConfig extends FluentGlobalTypes {
      /**
       * Sets properties which are then passed to the MBean Server Lookup implementation specified.
       *
       * @param properties properties to pass to the MBean Server Lookup
       */
      GlobalJmxStatisticsConfig withProperties(Properties properties);

      GlobalJmxStatisticsConfig addProperty(String key, String value);

      /**
       * If JMX statistics are enabled then all 'published' JMX objects will appear under this name.
       * This is optional, if not specified an object name will be created for you by default.
       *
       * @param jmxDomain
       */
      GlobalJmxStatisticsConfig jmxDomain(String jmxDomain);

      /**
       * Instance of class that will attempt to locate a JMX MBean server to bind to
       *
       * @param mbeanServerLookupClass MBean Server Lookup class implementation
       */
      GlobalJmxStatisticsConfig mBeanServerLookupClass(Class<? extends MBeanServerLookup> mbeanServerLookupClass);

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
       * Sets the instance of the {@link org.infinispan.jmx.MBeanServerLookup} class to be used to bound JMX MBeans to.
       *
       * @param mBeanServerLookupInstance An instance of {@link org.infinispan.jmx.MBeanServerLookup}
       */
      GlobalJmxStatisticsConfig mBeanServerLookup(MBeanServerLookup mBeanServerLookupInstance);

      GlobalJmxStatisticsConfig disable();
   }

   /**
    * Configures executor factory.
    */
   @Deprecated public static interface ExecutorFactoryConfig<T> extends FluentGlobalTypes {
      /**
       * Specify factory class for executor
       *
       * @param factory clazz
       * @return this ExecutorFactoryConfig
       */
      ExecutorFactoryConfig<T> factory(Class<? extends T> factory);

      /**
       * Add key/value property pair to this executor factory configuration
       *
       * @param key   property key
       * @param value property value
       * @return previous value if exists, null otherwise
       */
      ExecutorFactoryConfig<T> addProperty(String key, String value);

      /**
       * Set key/value properties to this executor factory configuration
       *
       * @param props Properties
       * @return this ExecutorFactoryConfig
       */
      ExecutorFactoryConfig<T> withProperties(Properties props);
   }

   @Deprecated public static interface ShutdownConfig extends FluentGlobalTypes {

      ShutdownConfig hookBehavior(GlobalConfiguration.ShutdownHookBehavior hookBehavior);
   }

}

@Deprecated
interface FluentGlobalTypes {

   FluentGlobalConfiguration.TransportConfig transport();

   /**
    * This method allows configuration of the global, or cache manager level,
    * jmx statistics. When this method is called, it automatically enables
    * global jmx statistics. So, if you want it to be disabled, make sure you call
    * {@link org.infinispan.config.FluentGlobalConfiguration.GlobalJmxStatisticsConfig#disable()}
    */
   FluentGlobalConfiguration.GlobalJmxStatisticsConfig globalJmxStatistics();

   FluentGlobalConfiguration.SerializationConfig serialization();

//   FluentGlobalConfiguration.ExecutorFactoryConfig<ExecutorFactory> asyncTransportExecutor();
//
//   FluentGlobalConfiguration.ExecutorFactoryConfig<ExecutorFactory> asyncListenerExecutor();

   FluentGlobalConfiguration.ExecutorFactoryConfig<ScheduledExecutorFactory> evictionScheduledExecutor();

   FluentGlobalConfiguration.ExecutorFactoryConfig<ScheduledExecutorFactory> replicationQueueScheduledExecutor();

   FluentGlobalConfiguration.ShutdownConfig shutdown();

   GlobalConfiguration build();
}
@Deprecated
abstract class AbstractConfigurationBeanWithGCR extends AbstractConfigurationBean implements FluentGlobalTypes {

   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = -5124687543159561028L;

   GlobalComponentRegistry gcr = null;

   GlobalConfiguration globalConfig;

   @Inject
   public void inject(GlobalComponentRegistry gcr, GlobalConfiguration globalConfig) {
      this.gcr = gcr;
      this.globalConfig = globalConfig;
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

   AbstractConfigurationBeanWithGCR setGlobalConfiguration(GlobalConfiguration globalConfig) {
      this.globalConfig = globalConfig;
      return this;
   }

   @Override
   public FluentGlobalConfiguration.TransportConfig transport() {
      return globalConfig.transport;
   }

   @Override
   public FluentGlobalConfiguration.GlobalJmxStatisticsConfig globalJmxStatistics() {
      globalConfig.globalJmxStatistics.setEnabled(true);
      return globalConfig.globalJmxStatistics;
   }

   @Override
   public FluentGlobalConfiguration.SerializationConfig serialization() {
      return globalConfig.serialization;
   }


   @Override
   public FluentGlobalConfiguration.ExecutorFactoryConfig<ScheduledExecutorFactory> evictionScheduledExecutor() {
      return globalConfig.evictionScheduledExecutor;
   }

   @Override
   public FluentGlobalConfiguration.ExecutorFactoryConfig<ScheduledExecutorFactory> replicationQueueScheduledExecutor() {
      return globalConfig.replicationQueueScheduledExecutor;
   }

   @Override
   public FluentGlobalConfiguration.ShutdownConfig shutdown() {
      return globalConfig.shutdown;
   }

   @Override
   public GlobalConfiguration build() {
      return globalConfig;
   }
}