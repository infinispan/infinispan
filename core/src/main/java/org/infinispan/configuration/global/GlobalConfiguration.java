package org.infinispan.configuration.global;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.infinispan.Version;

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
    * Default replication version, from {@link org.infinispan.Version#getVersionShort}.
    */
   public static final short DEFAULT_MARSHALL_VERSION = Version.getVersionShort();

   private final GlobalJmxStatisticsConfiguration globalJmxStatistics;
   private final TransportConfiguration transport;
   private final GlobalSecurityConfiguration security;
   private final SerializationConfiguration serialization;
   private final ShutdownConfiguration shutdown;
   private final Map<Class<?>, ?> modules;
   private final SiteConfiguration site;
   private final WeakReference<ClassLoader> cl;
   private final ThreadPoolConfiguration evictionThreadPool;
   private final ThreadPoolConfiguration listenerThreadPool;
   private final ThreadPoolConfiguration replicationQueueThreadPool;
   private final ThreadPoolConfiguration persistenceThreadPool;

   GlobalConfiguration(ThreadPoolConfiguration evictionThreadPool,
         ThreadPoolConfiguration listenerThreadPool,
         ThreadPoolConfiguration replicationQueueThreadPool,
         ThreadPoolConfiguration persistenceThreadPool,
         GlobalJmxStatisticsConfiguration globalJmxStatistics,
         TransportConfiguration transport, GlobalSecurityConfiguration security,
         SerializationConfiguration serialization, ShutdownConfiguration shutdown,
         List<?> modules, SiteConfiguration site,ClassLoader cl) {
      this.evictionThreadPool = evictionThreadPool;
      this.listenerThreadPool = listenerThreadPool;
      this.replicationQueueThreadPool = replicationQueueThreadPool;
      this.persistenceThreadPool = persistenceThreadPool;
      this.globalJmxStatistics = globalJmxStatistics;
      this.transport = transport;
      this.security = security;
      this.serialization = serialization;
      this.shutdown = shutdown;
      Map<Class<?>, Object> moduleMap = new HashMap<Class<?>, Object>();
      for(Object module : modules) {
         moduleMap.put(module.getClass(), module);
      }
      this.modules = Collections.unmodifiableMap(moduleMap);
      this.site = site;
      this.cl = new WeakReference<ClassLoader>(cl);
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
    * Look for a thread pool named as {@link #evictionThreadPool()} instead.
    */
   @Deprecated
   public ScheduledExecutorFactoryConfiguration evictionScheduledExecutor() {
      return null;
   }

   /**
    * @deprecated This method always returns null now.
    * Look for a thread pool named as {@link #replicationQueueThreadPool()} instead.
    */
   @Deprecated
   public ScheduledExecutorFactoryConfiguration replicationQueueScheduledExecutor() {
      return null;
   }

   public ThreadPoolConfiguration evictionThreadPool() {
      return evictionThreadPool;
   }

   public ThreadPoolConfiguration listenerThreadPool() {
      return listenerThreadPool;
   }

   public ThreadPoolConfiguration replicationQueueThreadPool() {
      return replicationQueueThreadPool;
   }

   public ThreadPoolConfiguration persistenceThreadPool() {
      return persistenceThreadPool;
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

   @SuppressWarnings("unchecked")
   public <T> T module(Class<T> moduleClass) {
      return (T)modules.get(moduleClass);
   }

   public Map<Class<?>, ?> modules() {
      return modules;
   }

   /**
    * Get the classloader in use by this configuration.
    */
   public ClassLoader classLoader() {
      return cl.get();
   }

   public SiteConfiguration sites() {
      return site;
   }

   @Override
   public String toString() {
      return "GlobalConfiguration{" +
            "listenerThreadPool=" + listenerThreadPool +
            ", evictionThreadPool=" + evictionThreadPool +
            ", persistenceThreadPool=" + persistenceThreadPool +
            ", replicationQueueThreadPool=" + replicationQueueThreadPool +
            ", globalJmxStatistics=" + globalJmxStatistics +
            ", transport=" + transport +
            ", security=" + security +
            ", serialization=" + serialization +
            ", shutdown=" + shutdown +
            ", modules=" + modules +
            ", site=" + site +
            ", cl=" + cl +
            '}';
   }

   /**
    * @deprecated This method always returns null now.
    * Look for a thread pool named as
    * {@link TransportConfiguration#totalOrderThreadPool()} instead.
    */
   @Deprecated
   public ExecutorFactoryConfiguration totalOrderExecutor() {
      return null;
   }

   public boolean isClustered() {
      return transport().transport() != null;
   }
}
