package org.infinispan.configuration.global;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
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
public class GlobalConfiguration implements ConfigurationInfo {

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

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition("infinispan");

   private List<ConfigurationInfo> subElements;

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
      JGroupsConfiguration jgroupsConfiguration = cacheContainerConfiguration.transport().jgroups();
      ThreadsConfiguration threads = cacheContainerConfiguration.threads();
      this.subElements = Arrays.asList(jgroupsConfiguration, threads, cacheContainerConfiguration);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
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

   public ThreadPoolConfiguration persistenceThreadPool() {
      return cacheContainerConfiguration.persistenceThreadPool();
   }

   public ThreadPoolConfiguration stateTransferThreadPool() {
      return cacheContainerConfiguration.stateTransferThreadPool();
   }

   public ThreadPoolConfiguration asyncThreadPool() {
      return cacheContainerConfiguration.asyncThreadPool();
   }

   public GlobalJmxStatisticsConfiguration globalJmxStatistics() {
      return cacheContainerConfiguration.globalJmxStatistics();
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

   public String asyncThreadPoolName() {
      return cacheContainer().asyncExecutor();
   }

   public String listenerThreadPoolName() {
      return cacheContainer().listenerExecutor();
   }

   public String expirationThreadPoolName() {
      return cacheContainer().expirationExecutor();
   }

   public String persistenceThreadPoolName() {
      return cacheContainer().persistenceExecutor();
   }

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
