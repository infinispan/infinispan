package org.infinispan.configuration.global;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.util.Features;

public class GlobalConfigurationBuilder implements GlobalConfigurationChildBuilder {

   private ClassLoader cl;
   private final CacheContainerConfigurationBuilder cacheContainerConfiguration;

   private final Map<Class<?>, Builder<?>> modules;
   private Features features;

   public GlobalConfigurationBuilder() {
      ClassLoader defaultCL = Thread.currentThread().getContextClassLoader();
      if (defaultCL == null) defaultCL = GlobalConfigurationBuilder.class.getClassLoader();
      this.cl = defaultCL;
      this.cacheContainerConfiguration = new CacheContainerConfigurationBuilder(this);
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
   public GlobalMetricsConfigurationBuilder metrics() {
      return cacheContainerConfiguration.metrics();
   }

   public GlobalTracingConfigurationBuilder tracing() {
      return cacheContainerConfiguration.tracing();
   }

   @Override
   public GlobalJmxConfigurationBuilder jmx() {
      return cacheContainerConfiguration.jmx();
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
   public ThreadPoolConfigurationBuilder asyncThreadPool() {
      return cacheContainerConfiguration.asyncThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder blockingThreadPool() {
      return cacheContainerConfiguration.blockingThreadPool();
   }

   @Override
   public ThreadPoolConfigurationBuilder nonBlockingThreadPool() {
      return cacheContainerConfiguration.nonBlockingThreadPool();
   }

   public GlobalConfigurationBuilder listenerThreadPoolName(String name) {
      cacheContainer().listenerExecutor(name);
      return this;
   }

   public GlobalConfigurationBuilder expirationThreadPoolName(String name) {
      cacheContainer().expirationExecutor(name);
      return this;
   }

   public GlobalConfigurationBuilder nonBlockingThreadPoolName(String name) {
      cacheContainer().nonBlockingExecutor(name);
      return this;
   }

   public GlobalConfigurationBuilder blockingThreadPoolName(String name) {
      cacheContainer().blockingExecutor(name);
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
      return List.copyOf(modules.values());
   }

   @Override
   public <T> T module(Class<T> moduleClass) {
      return (T) modules.get(moduleClass);
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

   @Override
   public GlobalConfigurationBuilder clearModules() {
      modules.clear();
      return this;
   }

   @Override
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

   public void validate() {
      features = new Features(cl);
      List<RuntimeException> validationExceptions = new ArrayList<>();
      try {
         cacheContainerConfiguration.validate();
      } catch (RuntimeException e) {
         validationExceptions.add(e);
      }
      modules.values().forEach(c -> {
         try {
            c.validate();
         } catch (RuntimeException e) {
            validationExceptions.add(e);
         }
      });
      CacheConfigurationException.fromMultipleRuntimeExceptions(validationExceptions).ifPresent(e -> {
         throw e;
      });
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
         builder.read(c, Combine.DEFAULT);
      }
      cacheContainerConfiguration.read(template.cacheContainer(), Combine.DEFAULT);
      tracing().read(template.tracing(), Combine.DEFAULT);
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
            ", features=" + features +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      GlobalConfigurationBuilder that = (GlobalConfigurationBuilder) o;
      return Objects.equals(cl, that.cl) && Objects.equals(cacheContainerConfiguration, that.cacheContainerConfiguration) && Objects.equals(modules, that.modules) && Objects.equals(features, that.features);
   }

   @Override
   public int hashCode() {
      return Objects.hash(cl, cacheContainerConfiguration, modules, features);
   }

   public ThreadsConfigurationBuilder threads() {
      return cacheContainerConfiguration.threads();
   }
}
