package org.infinispan.configuration.global;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.infinispan.commons.util.Features;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Configuration component that exposes the global configuration.
 *
 * @since 5.1
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Pete Muir
 * @author Pedro Ruivo
 *
 * @see <a href="../../../config.html#ce_infinispan_global">Configuration reference</a>
 *
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public class GlobalConfiguration {

   private final Map<Class<?>, ?> modules;
   private final ClassLoader cl;
   private final CacheContainerConfiguration cacheContainerConfiguration;
   private final Features features;

   GlobalConfiguration(CacheContainerConfiguration cacheContainerConfiguration,
                       List<?> modules,
                       ClassLoader cl, Features features) {
      this.cacheContainerConfiguration = cacheContainerConfiguration;
      Map<Class<?>, Object> moduleMap = new HashMap<>();
      for (Object module : modules) {
         moduleMap.put(module.getClass(), module);
      }
      this.modules = Map.copyOf(moduleMap);
      this.cl = cl;
      this.features = features;
   }

   /**
    * @return The {@link CacheContainerConfiguration} for the global configuration.
    */
   CacheContainerConfiguration cacheContainer() {
      return cacheContainerConfiguration;
   }

   /**
    * @return If the cache manager statistics are enabled.
    */
   public boolean statistics() {
      return cacheContainerConfiguration.statistics();
   }

   /**
    * @return The {@link ThreadPoolConfiguration} for the expiration thread pool.
    */
   public ThreadPoolConfiguration expirationThreadPool() {
      return cacheContainerConfiguration.expirationThreadPool();
   }

   /**
    * @return The {@link ThreadPoolConfiguration} for the listener thread pool.
    */
   public ThreadPoolConfiguration listenerThreadPool() {
      return cacheContainerConfiguration.listenerThreadPool();
   }

   /**
    * @return The {@link ThreadPoolConfiguration} for the non-blocking thread pool.
    */
   public ThreadPoolConfiguration nonBlockingThreadPool() {
      return cacheContainerConfiguration.nonBlockingThreadPool();
   }

   /**
    * @return The {@link ThreadPoolConfiguration} for the blocking thread pool.
    */
   public ThreadPoolConfiguration blockingThreadPool() {
      return cacheContainerConfiguration.blockingThreadPool();
   }

   /**
    * @return The {@link GlobalTracingConfiguration} for the global tracing.
    */
   public GlobalTracingConfiguration tracing() {
      return cacheContainerConfiguration.tracing();
   }

   /**
    * @return The {@link GlobalMetricsConfiguration} for the global metrics.
    */
   public GlobalMetricsConfiguration metrics() {
      return cacheContainerConfiguration.metrics();
   }

   /**
    * @return The {@link GlobalJmxConfiguration} for the global JMX.
    */
   public GlobalJmxConfiguration jmx() {
      return cacheContainerConfiguration.jmx();
   }

   /**
    * @return The name of the cache manager.
    */
   public String cacheManagerName() {
      return cacheContainerConfiguration.cacheManagerName();
   }

   /**
    * @return The {@link TransportConfiguration} for the global transport.
    */
   public TransportConfiguration transport() {
      return cacheContainerConfiguration.transport();
   }

   /**
    * @return The {@link GlobalSecurityConfiguration} for the global security.
    */
   public GlobalSecurityConfiguration security() {
      return cacheContainerConfiguration.security();
   }

   /**
    * @return The {@link SerializationConfiguration} for the global serialization.
    */
   public SerializationConfiguration serialization() {
      return cacheContainerConfiguration.serialization();
   }

   /**
    * @return The {@link ShutdownConfiguration} for the global shutdown.
    */
   public ShutdownConfiguration shutdown() {
      return cacheContainerConfiguration.shutdown();
   }

   /**
    * @return The {@link GlobalStateConfiguration} for the global state.
    */
   public GlobalStateConfiguration globalState() {
      return cacheContainerConfiguration.globalState();
   }

   /**
    * @return The name of the non-blocking thread pool.
    */
   public String nonBlockingThreadPoolName() {
      return cacheContainer().nonBlockingExecutor();
   }

   /**
    * @return The name of the listener thread pool.
    */
   public String listenerThreadPoolName() {
      return cacheContainer().listenerExecutor();
   }

   /**
    * @return The name of the expiration thread pool.
    */
   public String expirationThreadPoolName() {
      return cacheContainer().expirationExecutor();
   }

   /**
    * @return The name of the blocking thread pool.
    */
   public String blockingThreadPoolName() {
      return cacheContainer().blockingExecutor();
   }

   /**
    * Get a specific module's configuration.
    *
    * @param moduleClass The class of the module to retrieve.
    * @param <T> The type of the module configuration.
    * @return The module configuration, or {@code null} if not found.
    */
   @SuppressWarnings("unchecked")
   public <T> T module(Class<T> moduleClass) {
      return (T) modules.get(moduleClass);
   }

   /**
    * @return A map of all module configurations.
    */
   public Map<Class<?>, ?> modules() {
      return modules;
   }

   /**
    * Get the classloader in use by this configuration.
    * @return The {@link ClassLoader} in use.
    */
   public ClassLoader classLoader() {
      return cl;
   }

   /**
    * @return The default cache name.
    */
   public Optional<String> defaultCacheName() {
      return Optional.ofNullable(cacheContainerConfiguration.defaultCacheName());
   }

   /**
    * @return The {@link Features} instance for this configuration.
    */
   public Features features() {
      return features;
   }

   /**
    * @return True if the transport is configured, meaning this is a clustered setup.
    */
   public boolean isClustered() {
      return transport().transport() != null;
   }

   /**
    * Returns true if this node is configured as a zero-capacity node.
    * If the node is zero-capacity node, it won't hold any data except for replicated caches
    *
    * @return true or false
    */
   public boolean isZeroCapacityNode() {
      return cacheContainerConfiguration.getZeroCapacityNode();
   }

   /**
    * @return The container memory configuration.
    */
   public Map<String, ContainerMemoryConfiguration> getMemoryContainer() {
      return cacheContainerConfiguration.containerMemoryConfiguration();
   }

   @Override
   public String toString() {
      return "GlobalConfiguration{" +
            ", modules=" + modules +
            ", cl=" + cl +
            '}';
   }
}
