package org.infinispan.configuration.global;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.infinispan.Version;
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
   private static final String ZERO_CAPACITY_NODE_FEATURE = "zero-capacity-node";


   /**
    * Default replication version, from {@link org.infinispan.Version#getVersionShort}.
    *
    * @deprecated Since 9.4, use {@code Version.getVersionShort()} instead.
    */
   @Deprecated
   public static final short DEFAULT_MARSHALL_VERSION = Version.getVersionShort();

   private final GlobalJmxStatisticsConfiguration globalJmxStatistics;
   private final TransportConfiguration transport;
   private final GlobalSecurityConfiguration security;
   private final SerializationConfiguration serialization;
   private final ShutdownConfiguration shutdown;
   private final GlobalStateConfiguration globalState;
   private final Map<Class<?>, ?> modules;
   private final SiteConfiguration site;
   private final ClassLoader cl;
   private final ThreadPoolConfiguration expirationThreadPool;
   private final ThreadPoolConfiguration listenerThreadPool;
   private final ThreadPoolConfiguration replicationQueueThreadPool;
   private final ThreadPoolConfiguration persistenceThreadPool;
   private final ThreadPoolConfiguration stateTransferThreadPool;
   private final ThreadPoolConfiguration asyncThreadPool;
   private final Optional<String> defaultCacheName;
   private final Features features;
   private final boolean zeroCapacityNode;

   GlobalConfiguration(ThreadPoolConfiguration expirationThreadPool,
                       ThreadPoolConfiguration listenerThreadPool,
                       ThreadPoolConfiguration replicationQueueThreadPool,
                       ThreadPoolConfiguration persistenceThreadPool,
                       ThreadPoolConfiguration stateTransferThreadPool,
                       ThreadPoolConfiguration asyncThreadPool,
                       GlobalJmxStatisticsConfiguration globalJmxStatistics,
                       TransportConfiguration transport, GlobalSecurityConfiguration security,
                       SerializationConfiguration serialization, ShutdownConfiguration shutdown,
                       GlobalStateConfiguration globalState,
                       List<?> modules, SiteConfiguration site,
                       Optional<String> defaultCacheName,
                       ClassLoader cl, Features features,
                       boolean zeroCapacityNode) {
      this.expirationThreadPool = expirationThreadPool;
      this.listenerThreadPool = listenerThreadPool;
      this.replicationQueueThreadPool = replicationQueueThreadPool;
      this.persistenceThreadPool = persistenceThreadPool;
      this.stateTransferThreadPool = stateTransferThreadPool;
      this.asyncThreadPool = asyncThreadPool;
      this.globalJmxStatistics = globalJmxStatistics;
      this.transport = transport;
      this.security = security;
      this.serialization = serialization;
      this.shutdown = shutdown;
      this.globalState = globalState;
      Map<Class<?>, Object> moduleMap = new HashMap<>();
      for (Object module : modules) {
         moduleMap.put(module.getClass(), module);
      }
      this.modules = Collections.unmodifiableMap(moduleMap);
      this.site = site;
      this.defaultCacheName = defaultCacheName;
      this.cl = cl;
      this.features = features;
      this.zeroCapacityNode = features.isAvailable(ZERO_CAPACITY_NODE_FEATURE) ? zeroCapacityNode : false;
   }

   /**
    * @deprecated This method always returns null now.
    * Look for a thread pool named as {@link #listenerThreadPool()} instead.
    */
   @Deprecated
   public ExecutorFactoryConfiguration asyncListenerExecutor() {
      return null;
   }

   /**
    * @deprecated This method always returns null now.
    * Look for a thread pool named as {@link #persistenceThreadPool()} instead.
    */
   @Deprecated
   public ExecutorFactoryConfiguration persistenceExecutor() {
      return null;
   }

   /**
    * @deprecated This method always returns null now.
    * Look for a thread pool named as {@link TransportConfiguration#transportThreadPool()}
    * instead.
    */
   @Deprecated
   public ExecutorFactoryConfiguration asyncTransportExecutor() {
      return null;
   }

   /**
    * @deprecated This method always returns null now.
    * Look for a thread pool named as
    * {@link TransportConfiguration#remoteCommandThreadPool()} instead.
    */
   @Deprecated
   public ExecutorFactoryConfiguration remoteCommandsExecutor() {
      return null;
   }

   /**
    * @deprecated This method always returns null now.
    * Look for a thread pool named as {@link #expirationThreadPool()} instead.
    */
   @Deprecated
   public ScheduledExecutorFactoryConfiguration evictionScheduledExecutor() {
      return null;
   }

   public ThreadPoolConfiguration expirationThreadPool() {
      return expirationThreadPool;
   }

   /**
    * @deprecated Use {@link #expirationThreadPool} instead
    */
   @Deprecated
   public ThreadPoolConfiguration evictionThreadPool() {
      return expirationThreadPool;
   }

   public ThreadPoolConfiguration listenerThreadPool() {
      return listenerThreadPool;
   }

   /**
    * @deprecated Since 9.0, no longer used.
    */
   @Deprecated
   public ThreadPoolConfiguration replicationQueueThreadPool() {
      return replicationQueueThreadPool;
   }

   public ThreadPoolConfiguration persistenceThreadPool() {
      return persistenceThreadPool;
   }

   public ThreadPoolConfiguration stateTransferThreadPool() {
      return stateTransferThreadPool;
   }

   public ThreadPoolConfiguration asyncThreadPool() {
      return asyncThreadPool;
   }

   public GlobalJmxStatisticsConfiguration globalJmxStatistics() {
      return globalJmxStatistics;
   }

   public TransportConfiguration transport() {
      return transport;
   }

   public GlobalSecurityConfiguration security() {
      return security;
   }

   public SerializationConfiguration serialization() {
      return serialization;
   }

   public ShutdownConfiguration shutdown() {
      return shutdown;
   }

   public GlobalStateConfiguration globalState() {
      return globalState;
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
      return defaultCacheName;
   }

   public Features features() {
      return features;
   }

   @Override
   public String toString() {
      return "GlobalConfiguration{" +
            "listenerThreadPool=" + listenerThreadPool +
            ", expirationThreadPool=" + expirationThreadPool +
            ", persistenceThreadPool=" + persistenceThreadPool +
            ", stateTransferThreadPool=" + stateTransferThreadPool +
            ", replicationQueueThreadPool=" + replicationQueueThreadPool +
            ", globalJmxStatistics=" + globalJmxStatistics +
            ", transport=" + transport +
            ", security=" + security +
            ", serialization=" + serialization +
            ", shutdown=" + shutdown +
            ", globalState=" + globalState +
            ", modules=" + modules +
            ", site=" + site +
            ", defaultCacheName=" + defaultCacheName +
            ", cl=" + cl +
            ", zeroCapacityNode=" + zeroCapacityNode +
            '}';
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
      return zeroCapacityNode;
   }
}
