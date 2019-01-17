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
   private final TransportConfigurationBuilder transport;
   private final GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics;
   private final SerializationConfigurationBuilder serialization;
   private final GlobalSecurityConfigurationBuilder security;
   private final ThreadPoolConfigurationBuilder expirationThreadPool;
   private final ThreadPoolConfigurationBuilder listenerThreadPool;
   private final ThreadPoolConfigurationBuilder replicationQueueThreadPool;
   private final ThreadPoolConfigurationBuilder persistenceThreadPool;
   private final ThreadPoolConfigurationBuilder stateTransferThreadPool;
   private final ThreadPoolConfigurationBuilder asyncThreadPool;
   private final ShutdownConfigurationBuilder shutdown;
   private final GlobalStateConfigurationBuilder globalState;
   private final Map<Class<?>, Builder<?>> modules;
   private final SiteConfigurationBuilder site;
   private Optional<String> defaultCacheName;
   private boolean zeroCapacityNode;
   private Features features;

   public GlobalConfigurationBuilder() {
      // In OSGi contexts the TCCL should not be used. Use the infinispan-core bundle as default instead.
      ClassLoader defaultCL = null;
      if (!Util.isOSGiContext()) defaultCL = Thread.currentThread().getContextClassLoader();
      if (defaultCL == null) defaultCL = GlobalConfigurationBuilder.class.getClassLoader();
      this.cl = defaultCL;
      this.transport = new TransportConfigurationBuilder(this);
      this.globalJmxStatistics = new GlobalJmxStatisticsConfigurationBuilder(this);
      this.serialization = new SerializationConfigurationBuilder(this);
      this.security = new GlobalSecurityConfigurationBuilder(this);
      this.shutdown = new ShutdownConfigurationBuilder(this);
      this.globalState = new GlobalStateConfigurationBuilder(this);
      this.site = new SiteConfigurationBuilder(this);
      this.expirationThreadPool = new ThreadPoolConfigurationBuilder(this);
      this.listenerThreadPool = new ThreadPoolConfigurationBuilder(this);
      this.replicationQueueThreadPool = new ThreadPoolConfigurationBuilder(this);
      this.persistenceThreadPool = new ThreadPoolConfigurationBuilder(this);
      this.stateTransferThreadPool = new ThreadPoolConfigurationBuilder(this);
      this.asyncThreadPool = new ThreadPoolConfigurationBuilder(this);
      this.modules = new LinkedHashMap();
      this.defaultCacheName = Optional.empty();
      this.zeroCapacityNode = false;
   }

   /**
    * Helper method that gets you a default constructed GlobalConfiguration, preconfigured to use the default clustering
    * stack.
    *
    * @return a new global configuration
    */
   public GlobalConfigurationBuilder clusteredDefault() {
      transport().
         defaultTransport()
         .clearProperties();
      return this;
   }

   /**
    * Helper method that gets you a default constructed GlobalConfiguration, preconfigured for use in LOCAL mode
    *
    * @return a new global configuration
    */
   public GlobalConfigurationBuilder nonClusteredDefault() {
      transport()
         .transport(null)
         .clearProperties();
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
      return transport;
   }


   @Override
   public GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics() {
      return globalJmxStatistics;
   }

   @Override
   public SerializationConfigurationBuilder serialization() {
      return serialization;
   }

   @Deprecated
   @Override
   public ThreadPoolConfigurationBuilder evictionThreadPool() {
      return expirationThreadPool;
   }

   @Override
   public ThreadPoolConfigurationBuilder expirationThreadPool() {
      return expirationThreadPool;
   }

   @Override
   public ThreadPoolConfigurationBuilder listenerThreadPool() {
      return listenerThreadPool;
   }

   @Deprecated
   @Override
   public ThreadPoolConfigurationBuilder replicationQueueThreadPool() {
      return replicationQueueThreadPool;
   }

   @Override
   public ThreadPoolConfigurationBuilder persistenceThreadPool() {
      return persistenceThreadPool;
   }

   @Override
   public ThreadPoolConfigurationBuilder stateTransferThreadPool() {
      return stateTransferThreadPool;
   }

   @Override
   public ThreadPoolConfigurationBuilder asyncThreadPool() {
      return asyncThreadPool;
   }

   @Override
   public GlobalSecurityConfigurationBuilder security() {
      return security;
   }

   @Override
   public ShutdownConfigurationBuilder shutdown() {
      return shutdown;
   }

   @Override
   public List<Builder<?>> modules() {
      return Collections.unmodifiableList(new ArrayList<>(modules.values()));
   }

   public <T> T module(Class<T> moduleClass) {
      return (T)modules.get(moduleClass);
   }

   public GlobalConfigurationBuilder zeroCapacityNode(boolean zeroCapacityNode) {
      this.zeroCapacityNode = zeroCapacityNode;
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
         this.modules.put(klass, builder);
         return builder;
      } catch (Exception e) {
         throw new CacheConfigurationException("Could not instantiate module configuration builder '" + klass.getName() + "'", e);
      }
   }

   @Override
   public GlobalStateConfigurationBuilder globalState() {
      return globalState;
   }

   @Override
   public GlobalConfigurationBuilder defaultCacheName(String defaultCacheName) {
      this.defaultCacheName = Optional.of(defaultCacheName);
      return this;
   }

   public Optional<String> defaultCacheName() {
      return defaultCacheName;
   }

   @SuppressWarnings("unchecked")
   public void validate() {
      features = new Features(cl);
      List<RuntimeException> validationExceptions = new ArrayList<>();
      Arrays.asList(
            expirationThreadPool,
            listenerThreadPool,
            replicationQueueThreadPool,
            persistenceThreadPool,
            stateTransferThreadPool,
            asyncThreadPool,
            globalJmxStatistics,
            transport,
            security,
            serialization,
            shutdown,
            globalState,
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
            expirationThreadPool.create(),
            listenerThreadPool.create(),
            replicationQueueThreadPool.create(),
            persistenceThreadPool.create(),
            stateTransferThreadPool.create(),
            asyncThreadPool.create(),
            globalJmxStatistics.create(),
            transport.create(),
            security.create(),
            serialization.create(),
            shutdown.create(),
            globalState.create(),
            modulesConfig,
            site.create(),
            defaultCacheName,
            cl,
            features,
            zeroCapacityNode);
   }

   public GlobalConfigurationBuilder read(GlobalConfiguration template) {
      this.cl = template.classLoader();
      this.defaultCacheName = template.defaultCacheName();

      for (Object c : template.modules().values()) {
         BuiltBy builtBy = c.getClass().getAnnotation(BuiltBy.class);
         Builder<Object> builder = this.addModule(builtBy.value());
         builder.read(c);
      }

      expirationThreadPool.read(template.expirationThreadPool());
      listenerThreadPool.read(template.listenerThreadPool());
      replicationQueueThreadPool.read(template.replicationQueueThreadPool());
      persistenceThreadPool.read(template.persistenceThreadPool());
      stateTransferThreadPool.read(template.stateTransferThreadPool());
      asyncThreadPool.read(template.asyncThreadPool());
      globalJmxStatistics.read(template.globalJmxStatistics());
      security.read(template.security());
      serialization.read(template.serialization());
      shutdown.read(template.shutdown());
      globalState.read(template.globalState());
      transport.read(template.transport());
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
            "expirationThreadPool=" + expirationThreadPool +
            ", listenerExecutorThreadPool=" + listenerThreadPool +
            ", cl=" + cl +
            ", transport=" + transport +
            ", globalJmxStatistics=" + globalJmxStatistics +
            ", serialization=" + serialization +
            ", replicationQueueThreadPool=" + replicationQueueThreadPool +
            ", persistenceThreadPool=" + persistenceThreadPool +
            ", stateTransferThreadPool=" + stateTransferThreadPool +
            ", asyncThreadPool=" + asyncThreadPool +
            ", security=" + security +
            ", shutdown=" + shutdown +
            ", globalState=" + globalState +
            ", site=" + site +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      GlobalConfigurationBuilder that = (GlobalConfigurationBuilder) o;

      if (!expirationThreadPool.equals(that.expirationThreadPool))
         return false;
      if (!listenerThreadPool.equals(that.listenerThreadPool))
         return false;
      if (!replicationQueueThreadPool.equals(that.replicationQueueThreadPool))
         return false;
      if (!persistenceThreadPool.equals(that.persistenceThreadPool))
         return false;
      if (!stateTransferThreadPool.equals(that.stateTransferThreadPool))
         return false;
      if (!asyncThreadPool.equals(that.asyncThreadPool))
         return false;
      if (cl != null ? !cl.equals(that.cl) : that.cl != null)
         return false;
      if (!globalJmxStatistics.equals(that.globalJmxStatistics))
         return false;
      if (!serialization.equals(that.serialization))
         return false;
      if (!shutdown.equals(that.shutdown))
         return false;
      if (!site.equals(that.site))
         return false;
      if (!security.equals(that.security))
         return false;
      if (!globalState.equals(that.globalState))
         return false;
      if (!defaultCacheName.equals(that.defaultCacheName))
         return false;

      return !transport.equals(that.transport);
   }

   @Override
   public int hashCode() {
      int result = cl != null ? cl.hashCode() : 0;
      result = 31 * result + (transport.hashCode());
      result = 31 * result + (globalJmxStatistics.hashCode());
      result = 31 * result + (serialization.hashCode());
      result = 31 * result + (expirationThreadPool.hashCode());
      result = 31 * result + (listenerThreadPool.hashCode());
      result = 31 * result + (replicationQueueThreadPool.hashCode());
      result = 31 * result + (persistenceThreadPool.hashCode());
      result = 31 * result + (stateTransferThreadPool.hashCode());
      result = 31 * result + (asyncThreadPool.hashCode());
      result = 31 * result + (shutdown.hashCode());
      result = 31 * result + (site.hashCode());
      result = 31 * result + (security.hashCode());
      result = 31 * result + (globalState.hashCode());
      result = 31 * result + (modules.hashCode());
      result = 31 * result + (defaultCacheName.hashCode());
      return result;
   }

}
