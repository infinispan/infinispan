package org.infinispan.configuration.global;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.util.Features;
import org.infinispan.commons.util.Util;

public class GlobalConfigurationBuilder implements GlobalConfigurationChildBuilder {

   private ClassLoader cl;
   private final CacheContainerConfigurationBuilder cacheContainerConfiguration;

   private final Map<Class<?>, Builder<?>> modules;
   private final SiteConfigurationBuilder site;
   private Features features;

   public GlobalConfigurationBuilder() {
      // In OSGi contexts the TCCL should not be used. Use the infinispan-core bundle as default instead.
      ClassLoader defaultCL = null;
      if (!Util.isOSGiContext()) defaultCL = Thread.currentThread().getContextClassLoader();
      if (defaultCL == null) defaultCL = GlobalConfigurationBuilder.class.getClassLoader();
      this.cl = defaultCL;
      this.cacheContainerConfiguration = new CacheContainerConfigurationBuilder(this);
      this.site = new SiteConfigurationBuilder(this);
      this.modules = new LinkedHashMap<>();
   }

   public CacheContainerConfigurationBuilder cacheContainer() {
      return cacheContainerConfiguration;
   }

   /**
    * Helper method that gets you a default constructed GlobalConfiguration, preconfigured to use the default clustering
    * stack.
    *
    * @return a new global configuration
    */
   public GlobalConfigurationBuilder clusteredDefault() {
      cacheContainerConfiguration.clusteredDefault();
      return this;
   }

   /**
    * Helper method that gets you a default constructed GlobalConfiguration, preconfigured for use in LOCAL mode
    *
    * @return a new global configuration
    */
   public GlobalConfigurationBuilder nonClusteredDefault() {
      cacheContainerConfiguration.nonClusteredDefault();
      return this;
   }

   protected ClassLoader getClassLoader() {
      return cl;
   }

   public GlobalConfigurationBuilder classLoader(ClassLoader cl) {
      this.cl = cl;
      return this;
   }

   @Override
   public TransportConfigurationBuilder transport() {
      return cacheContainerConfiguration.transport();
   }

   public GlobalConfigurationBuilder cacheManagerName(String name) {
      cacheContainerConfiguration.name(name);
      return this;
   }

   @Override
   public GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics() {
      return cacheContainerConfiguration.globalJmxStatistics();
   }

   @Override
   public SerializationConfigurationBuilder serialization() {
      return cacheContainerConfiguration.serialization();
   }

   @Override
   public ThreadPoolConfigurationBuilder expirationThreadPool() {
      return cacheContainerConfiguration.expirationThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder listenerThreadPool() {
      return cacheContainerConfiguration.listenerThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder persistenceThreadPool() {
      return cacheContainerConfiguration.persistenceThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder stateTransferThreadPool() {
      return cacheContainerConfiguration.stateTransferThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder asyncThreadPool() {
      return cacheContainerConfiguration.asyncThreadPool();
   }

   public GlobalConfigurationBuilder asyncThreadPoolName(String name) {
      cacheContainer().asyncExecutor(name);
      return this;
   }

   public GlobalConfigurationBuilder listenerThreadPoolName(String name) {
      cacheContainer().listenerExecutor(name);
      return this;
   }

   public GlobalConfigurationBuilder expirationThreadPoolName(String name) {
      cacheContainer().expirationExecutor(name);
      return this;
   }

   public GlobalConfigurationBuilder persistenceThreadPoolName(String name) {
      cacheContainer().persistenceExecutor(name);
      return this;
   }

   public GlobalConfigurationBuilder stateTransferThreadPoolName(String name) {
      cacheContainer().stateTransferExecutor(name);
      return this;
   }

   @Override
   public GlobalSecurityConfigurationBuilder security() {
      return cacheContainerConfiguration.security();
   }

   @Override
   public ShutdownConfigurationBuilder shutdown() {
      return cacheContainerConfiguration.shutdown();
   }

   @Override
   public List<Builder<?>> modules() {
      return Collections.unmodifiableList(new ArrayList<>(modules.values()));
   }

   public <T> T module(Class<T> moduleClass) {
      return (T)modules.get(moduleClass);
   }

   /**
    * Set the zero capacity node to true to configure a global capacity factor 0.0f for every distributed cache.
    * The node will join the cluster but won't keep data on it.
    * However, this flag does not affect replicated caches.
    * Replicated caches will continue to keep copies of the data in this node.
    * Use only distributed caches to make the best use of this feature.
    *
    * @param zeroCapacityNode value, true or false
    * @return GlobalConfigurationBuilder instance
    */
   public GlobalConfigurationBuilder zeroCapacityNode(boolean zeroCapacityNode) {
      cacheContainerConfiguration.zeroCapacityNode(zeroCapacityNode);
      return this;
   }

   public GlobalConfigurationBuilder clearModules() {
      modules.clear();
      return this;
   }

   @Override
   public SiteConfigurationBuilder site() {
      return site;
   }

   public <T extends Builder<?>> T addModule(Class<T> klass) {
      try {
         Constructor<T> constructor = klass.getDeclaredConstructor(GlobalConfigurationBuilder.class);
         T builder = constructor.newInstance(this);
         T existing = (T) this.modules.putIfAbsent(klass, builder);
         return existing != null ? existing : builder;
      } catch (Exception e) {
         throw new CacheConfigurationException("Could not instantiate module configuration builder '" + klass.getName() + "'", e);
      }
   }

   @Override
   public GlobalStateConfigurationBuilder globalState() {
      return cacheContainerConfiguration.globalState();
   }

   @Override
   public GlobalConfigurationBuilder defaultCacheName(String defaultCacheName) {
      cacheContainerConfiguration.defaultCache(defaultCacheName);
      return this;
   }

   public Optional<String> defaultCacheName() {
      return Optional.ofNullable(cacheContainerConfiguration.defaultCacheName());
   }

   @SuppressWarnings("unchecked")
   public void validate() {
      features = new Features(cl);
      List<RuntimeException> validationExceptions = new ArrayList<>();
      Arrays.asList(
            cacheContainerConfiguration,
            site
      ).forEach(c -> {
         try {
            c.validate();
         } catch (RuntimeException e) {
            validationExceptions.add(e);
         }
      });
      modules.values().forEach(c -> {
         try {
            c.validate();
         } catch (RuntimeException e) {
            validationExceptions.add(e);
         }
      });
      CacheConfigurationException.fromMultipleRuntimeExceptions(validationExceptions).ifPresent(e -> { throw e; });
   }

   @Override
   public GlobalConfiguration build() {
      validate();
      List<Object> modulesConfig = new LinkedList<>();
      for (Builder<?> module : modules.values())
         modulesConfig.add(module.create());
      return new GlobalConfiguration(
            cacheContainerConfiguration.create(),
            modulesConfig,
            site.create(),
            cl,
            features);
   }

   public Features getFeatures() {
      return features;
   }

   public GlobalConfigurationBuilder read(GlobalConfiguration template) {
      this.cl = template.classLoader();

      for (Object c : template.modules().values()) {
         BuiltBy builtBy = c.getClass().getAnnotation(BuiltBy.class);
         Builder<Object> builder = this.addModule(builtBy.value());
         builder.read(c);
      }
      cacheContainerConfiguration.read(template.cacheContainer());
      site.read(template.sites());
      return this;
   }

   public static GlobalConfigurationBuilder defaultClusteredBuilder() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.transport().defaultTransport();
      return builder;
   }

   @Override
   public String toString() {
      return "GlobalConfigurationBuilder{" +
            "cl=" + cl +
            ", cacheContainerConfiguration=" + cacheContainerConfiguration +
            ", modules=" + modules +
            ", site=" + site +
            ", features=" + features +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      GlobalConfigurationBuilder builder = (GlobalConfigurationBuilder) o;

      if (cl != null ? !cl.equals(builder.cl) : builder.cl != null) return false;
      if (cacheContainerConfiguration != null ? !cacheContainerConfiguration.equals(builder.cacheContainerConfiguration) : builder.cacheContainerConfiguration != null)
         return false;
      if (modules != null ? !modules.equals(builder.modules) : builder.modules != null) return false;
      if (site != null ? !site.equals(builder.site) : builder.site != null) return false;
      return features != null ? features.equals(builder.features) : builder.features == null;
   }

   @Override
   public int hashCode() {
      int result = cl != null ? cl.hashCode() : 0;
      result = 31 * result + (cacheContainerConfiguration != null ? cacheContainerConfiguration.hashCode() : 0);
      result = 31 * result + (modules != null ? modules.hashCode() : 0);
      result = 31 * result + (site != null ? site.hashCode() : 0);
      result = 31 * result + (features != null ? features.hashCode() : 0);
      return result;
   }

   public ThreadsConfigurationBuilder threads() {
      return cacheContainerConfiguration.threads();
   }
}
