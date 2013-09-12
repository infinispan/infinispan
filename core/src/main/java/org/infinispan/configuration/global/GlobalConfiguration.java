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

   private final ExecutorFactoryConfiguration asyncListenerExecutor;
   private final ExecutorFactoryConfiguration persistenceExecutor;
   private final ExecutorFactoryConfiguration asyncTransportExecutor;
   private final ExecutorFactoryConfiguration remoteCommandsExecutor;
   private final ExecutorFactoryConfiguration totalOrderExecutor;
   private final ScheduledExecutorFactoryConfiguration evictionScheduledExecutor;
   private final ScheduledExecutorFactoryConfiguration replicationQueueScheduledExecutor;
   private final GlobalJmxStatisticsConfiguration globalJmxStatistics;
   private final TransportConfiguration transport;
   private final SerializationConfiguration serialization;
   private final ShutdownConfiguration shutdown;
   private final Map<Class<?>, ?> modules;
   private final SiteConfiguration site;
   private final WeakReference<ClassLoader> cl;

   GlobalConfiguration(ExecutorFactoryConfiguration asyncListenerExecutor,
         ExecutorFactoryConfiguration asyncTransportExecutor, ExecutorFactoryConfiguration remoteCommandsExecutor,
         ScheduledExecutorFactoryConfiguration evictionScheduledExecutor,
         ScheduledExecutorFactoryConfiguration replicationQueueScheduledExecutor, GlobalJmxStatisticsConfiguration globalJmxStatistics,
         TransportConfiguration transport, SerializationConfiguration serialization, ShutdownConfiguration shutdown,
         List<?> modules, SiteConfiguration site,ClassLoader cl, ExecutorFactoryConfiguration totalOrderExecutor, ExecutorFactoryConfiguration persistenceExecutor) {
      this.asyncListenerExecutor = asyncListenerExecutor;
      this.persistenceExecutor = persistenceExecutor;
      this.asyncTransportExecutor = asyncTransportExecutor;
      this.remoteCommandsExecutor = remoteCommandsExecutor;
      this.evictionScheduledExecutor = evictionScheduledExecutor;
      this.replicationQueueScheduledExecutor = replicationQueueScheduledExecutor;
      this.globalJmxStatistics = globalJmxStatistics;
      this.transport = transport;
      this.serialization = serialization;
      this.shutdown = shutdown;
      Map<Class<?>, Object> moduleMap = new HashMap<Class<?>, Object>();
      for(Object module : modules) {
         moduleMap.put(module.getClass(), module);
      }
      this.modules = Collections.unmodifiableMap(moduleMap);
      this.site = site;
      this.cl = new WeakReference<ClassLoader>(cl);
      this.totalOrderExecutor = totalOrderExecutor;
   }

   public ExecutorFactoryConfiguration asyncListenerExecutor() {
      return asyncListenerExecutor;
   }

   public ExecutorFactoryConfiguration persistenceExecutor() {
      return persistenceExecutor;
   }

   public ExecutorFactoryConfiguration asyncTransportExecutor() {
      return asyncTransportExecutor;
   }

   public ExecutorFactoryConfiguration remoteCommandsExecutor() {
      return remoteCommandsExecutor;
   }

   public ScheduledExecutorFactoryConfiguration evictionScheduledExecutor() {
      return evictionScheduledExecutor;
   }

   public ScheduledExecutorFactoryConfiguration replicationQueueScheduledExecutor() {
      return replicationQueueScheduledExecutor;
   }

   public GlobalJmxStatisticsConfiguration globalJmxStatistics() {
      return globalJmxStatistics;
   }

   public TransportConfiguration transport() {
      return transport;
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
            "asyncListenerExecutor=" + asyncListenerExecutor +
            ", asyncTransportExecutor=" + asyncTransportExecutor +
            ", remoteCommandsExecutor=" + remoteCommandsExecutor +
            ", evictionScheduledExecutor=" + evictionScheduledExecutor +
            ", replicationQueueScheduledExecutor=" + replicationQueueScheduledExecutor +
            ", globalJmxStatistics=" + globalJmxStatistics +
            ", transport=" + transport +
            ", serialization=" + serialization +
            ", shutdown=" + shutdown +
            ", modules=" + modules +
            ", site=" + site +
            ", cl=" + cl +
            ", totalOrderExecutor=" + totalOrderExecutor +
            '}';
   }

   public ExecutorFactoryConfiguration totalOrderExecutor() {
      return totalOrderExecutor;
   }

   public boolean isClustered() {
      return transport().transport() != null;
   }
}
