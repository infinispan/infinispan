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
 * <p>
 * Configuration component that exposes the global configuration.
 * </p>
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Pete Muir
 * @author Pedro Ruivo
 * @since 5.1
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

   CacheContainerConfiguration cacheContainer() {
      return cacheContainerConfiguration;
   }

   public boolean statistics() {
      return cacheContainerConfiguration.statistics();
   }

   public ThreadPoolConfiguration expirationThreadPool() {
      return cacheContainerConfiguration.expirationThreadPool();
   }

   public ThreadPoolConfiguration listenerThreadPool() {
      return cacheContainerConfiguration.listenerThreadPool();
   }

   public ThreadPoolConfiguration nonBlockingThreadPool() {
      return cacheContainerConfiguration.nonBlockingThreadPool();
   }

   public ThreadPoolConfiguration blockingThreadPool() {
      return cacheContainerConfiguration.blockingThreadPool();
   }

   public GlobalTracingConfiguration tracing() {
      return cacheContainerConfiguration.tracing();
   }

   public GlobalMetricsConfiguration metrics() {
      return cacheContainerConfiguration.metrics();
   }

   public GlobalJmxConfiguration jmx() {
      return cacheContainerConfiguration.jmx();
   }

   public String cacheManagerName() {
      return cacheContainerConfiguration.cacheManagerName();
   }

   public TransportConfiguration transport() {
      return cacheContainerConfiguration.transport();
   }

   public GlobalSecurityConfiguration security() {
      return cacheContainerConfiguration.security();
   }

   public SerializationConfiguration serialization() {
      return cacheContainerConfiguration.serialization();
   }

   public ShutdownConfiguration shutdown() {
      return cacheContainerConfiguration.shutdown();
   }

   public GlobalStateConfiguration globalState() {
      return cacheContainerConfiguration.globalState();
   }

   public String nonBlockingThreadPoolName() {
      return cacheContainer().nonBlockingExecutor();
   }

   public String listenerThreadPoolName() {
      return cacheContainer().listenerExecutor();
   }

   public String expirationThreadPoolName() {
      return cacheContainer().expirationExecutor();
   }

   public String blockingThreadPoolName() {
      return cacheContainer().blockingExecutor();
   }

   @SuppressWarnings("unchecked")
   public <T> T module(Class<T> moduleClass) {
      return (T) modules.get(moduleClass);
   }

   public Map<Class<?>, ?> modules() {
      return modules;
   }

   /**
    * Get the classloader in use by this configuration.
    */
   public ClassLoader classLoader() {
      return cl;
   }

   public Optional<String> defaultCacheName() {
      return Optional.ofNullable(cacheContainerConfiguration.defaultCacheName());
   }

   public Features features() {
      return features;
   }

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

   @Override
   public String toString() {
      return "GlobalConfiguration{" +
            ", modules=" + modules +
            ", cl=" + cl +
            '}';
   }
}
