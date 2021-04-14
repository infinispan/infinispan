package org.infinispan.configuration.global;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.infinispan.commons.util.Features;
import org.infinispan.commons.util.Version;
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

   /**
    * Default replication version, from {@link org.infinispan.commons.util.Version#getVersionShort}.
    *
    * @deprecated Since 9.4, use {@code Version.getVersionShort()} instead.
    */
   @Deprecated
   public static final short DEFAULT_MARSHALL_VERSION = Version.getVersionShort();

   private final Map<Class<?>, ?> modules;
   private final SiteConfiguration site;
   private final ClassLoader cl;
   private final CacheContainerConfiguration cacheContainerConfiguration;
   private final Features features;

   GlobalConfiguration(CacheContainerConfiguration cacheContainerConfiguration,
                       List<?> modules, SiteConfiguration site,
                       ClassLoader cl, Features features) {
      this.cacheContainerConfiguration = cacheContainerConfiguration;
      Map<Class<?>, Object> moduleMap = new HashMap<>();
      for (Object module : modules) {
         moduleMap.put(module.getClass(), module);
      }
      this.modules = Collections.unmodifiableMap(moduleMap);
      this.site = site;
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

   /**
    * @return An empty {@code ThreadPoolConfiguration}.
    * @deprecated Since 11.0, no longer used.
    */
   @Deprecated
   public ThreadPoolConfiguration persistenceThreadPool() {
      return cacheContainerConfiguration.persistenceThreadPool();
   }

   /**
    * @return An empty {@code ThreadPoolConfiguration}.
    * @deprecated Since 10.1, no longer used.
    */
   @Deprecated
   public ThreadPoolConfiguration stateTransferThreadPool() {
      return cacheContainerConfiguration.stateTransferThreadPool();
   }

   /**
    * @return An empty {@code ThreadPoolConfiguration}.
    * @deprecated Since 11.0, no longer used.
    */
   @Deprecated
   public ThreadPoolConfiguration asyncThreadPool() {
      return cacheContainerConfiguration.asyncThreadPool();
   }

   public ThreadPoolConfiguration nonBlockingThreadPool() {
      return cacheContainerConfiguration.nonBlockingThreadPool();
   }

   public ThreadPoolConfiguration blockingThreadPool() {
      return cacheContainerConfiguration.blockingThreadPool();
   }

   public GlobalMetricsConfiguration metrics() {
      return cacheContainerConfiguration.metrics();
   }

   public GlobalJmxConfiguration jmx() {
      return cacheContainerConfiguration.jmx();
   }

   /**
    * @deprecated Since 10.1.3. Use {@link #jmx()} instead. This will be removed in next major version.
    */
   @Deprecated
   public GlobalJmxConfiguration globalJmxStatistics() {
      return jmx();
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

   /**
    * @deprecated Since 11.0, no longer used.
    */
   @Deprecated
   public String asyncThreadPoolName() {
      return cacheContainer().asyncExecutor();
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

   /**
    * @deprecated Since 11.0, no longer used.
    */
   @Deprecated
   public String persistenceThreadPoolName() {
      return cacheContainer().persistenceExecutor();
   }

   public String blockingThreadPoolName() {
      return cacheContainer().blockingExecutor();
   }

   /**
    * @deprecated Since 10.1, no longer used.
    */
   @Deprecated
   public String stateTransferThreadPoolName() {
      return cacheContainer().stateTransferExecutor();
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

   public SiteConfiguration sites() {
      return site;
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
            ", site=" + site +
            ", cl=" + cl +
            '}';
   }
}
