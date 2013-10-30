package org.infinispan.configuration.global;

import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.CacheConfigurationException;
import static java.util.Arrays.asList;

public class GlobalConfigurationBuilder implements GlobalConfigurationChildBuilder {

   private WeakReference<ClassLoader> cl;
   private final TransportConfigurationBuilder transport;
   private final GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics;
   private final SerializationConfigurationBuilder serialization;
   private final ExecutorFactoryConfigurationBuilder asyncTransportExecutor;
   private final ExecutorFactoryConfigurationBuilder asyncListenerExecutor;
   private final ExecutorFactoryConfigurationBuilder persistenceExecutor;
   private final ExecutorFactoryConfigurationBuilder remoteCommandsExecutor;
   private final ExecutorFactoryConfigurationBuilder totalOrderExecutor;
   private final ScheduledExecutorFactoryConfigurationBuilder evictionScheduledExecutor;
   private final ScheduledExecutorFactoryConfigurationBuilder replicationQueueScheduledExecutor;
   private final GlobalSecurityConfigurationBuilder security;
   private final ShutdownConfigurationBuilder shutdown;
   private final List<Builder<?>> modules = new ArrayList<Builder<?>>();
   private final SiteConfigurationBuilder site;

   public GlobalConfigurationBuilder() {
      this.cl = new WeakReference<ClassLoader>(Thread.currentThread().getContextClassLoader());
      this.transport = new TransportConfigurationBuilder(this);
      this.globalJmxStatistics = new GlobalJmxStatisticsConfigurationBuilder(this);
      this.serialization = new SerializationConfigurationBuilder(this);
      this.asyncListenerExecutor = new ExecutorFactoryConfigurationBuilder(this);
      this.persistenceExecutor = new ExecutorFactoryConfigurationBuilder(this);
      this.asyncTransportExecutor = new ExecutorFactoryConfigurationBuilder(this);
      this.remoteCommandsExecutor = new ExecutorFactoryConfigurationBuilder(this);
      this.evictionScheduledExecutor = new ScheduledExecutorFactoryConfigurationBuilder(this);
      this.replicationQueueScheduledExecutor = new ScheduledExecutorFactoryConfigurationBuilder(this);
      this.security = new GlobalSecurityConfigurationBuilder(this);
      this.shutdown = new ShutdownConfigurationBuilder(this);
      this.site = new SiteConfigurationBuilder(this);
      //set a new executor by default, that allows to set the core number of threads and the keep alive time
      this.totalOrderExecutor = new ExecutorFactoryConfigurationBuilder(this);
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
         .clearProperties()
      .asyncTransportExecutor()
         .addProperty("threadNamePrefix", "asyncTransportThread");
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
      return cl.get();
   }

   public GlobalConfigurationBuilder classLoader(ClassLoader cl) {
      this.cl = new WeakReference<ClassLoader>(cl);
      return this;
   }

   @Override
   public TransportConfigurationBuilder transport() {
      return transport;
   }

   /**
    * This method allows configuration of the global, or cache manager level,
    * jmx statistics.
    */
   @Override
   public GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics() {
      return globalJmxStatistics;
   }

   @Override
   public SerializationConfigurationBuilder serialization() {
      return serialization;
   }

   @Override
   public ExecutorFactoryConfigurationBuilder asyncTransportExecutor() {
      return asyncTransportExecutor;
   }

   @Override
   public ExecutorFactoryConfigurationBuilder asyncListenerExecutor() {
      return asyncListenerExecutor;
   }

   @Override
   public ExecutorFactoryConfigurationBuilder persistenceExecutor() {
      return persistenceExecutor;
   }

   @Override
   public ExecutorFactoryConfigurationBuilder remoteCommandsExecutor() {
      return remoteCommandsExecutor;
   }

   @Override
   public ScheduledExecutorFactoryConfigurationBuilder evictionScheduledExecutor() {
      return evictionScheduledExecutor;
   }

   @Override
   public ScheduledExecutorFactoryConfigurationBuilder replicationQueueScheduledExecutor() {
      return replicationQueueScheduledExecutor;
   }

   @Override
   public GlobalSecurityConfigurationBuilder security() {
      return security;
   }

   @Override
   public ShutdownConfigurationBuilder shutdown() {
      return shutdown;
   }

   public List<Builder<?>> modules() {
      return modules;
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
         this.modules.add(builder);
         return builder;
      } catch (Exception e) {
         throw new CacheConfigurationException("Could not instantiate module configuration builder '" + klass.getName() + "'", e);
      }
   }

   public ExecutorFactoryConfigurationBuilder totalOrderExecutor() {
      return totalOrderExecutor;
   }

   @SuppressWarnings("unchecked")
   public void validate() {
      for (Builder<?> validatable : asList(asyncListenerExecutor, persistenceExecutor, asyncTransportExecutor,
            remoteCommandsExecutor, evictionScheduledExecutor, replicationQueueScheduledExecutor, globalJmxStatistics, transport,
            serialization, shutdown, site, totalOrderExecutor)) {
         validatable.validate();
      }
      for (Builder<?> m : modules) {
         m.validate();
      }
   }

   @Override
   public GlobalConfiguration build() {
      validate();
      List<Object> modulesConfig = new LinkedList<Object>();
      for (Builder<?> module : modules)
         modulesConfig.add(module.create());
      return new GlobalConfiguration(
            asyncListenerExecutor.create(),
            asyncTransportExecutor.create(),
            remoteCommandsExecutor.create(),
            evictionScheduledExecutor.create(),
            replicationQueueScheduledExecutor.create(),
            globalJmxStatistics.create(),
            transport.create(),
            security.create(),
            serialization.create(),
            shutdown.create(),
            modulesConfig,
            site.create(),
            cl.get(),
            totalOrderExecutor.create(),
            persistenceExecutor.create()
            );
   }

   public GlobalConfigurationBuilder read(GlobalConfiguration template) {
      this.cl = new WeakReference<ClassLoader>(template.classLoader());

      for (Object c : template.modules().values()) {
         BuiltBy builtBy = c.getClass().getAnnotation(BuiltBy.class);
         Builder<Object> builder = this.addModule(builtBy.value());
         builder.read(c);
      }

      asyncListenerExecutor.read(template.asyncListenerExecutor());
      persistenceExecutor.read(template.asyncListenerExecutor());
      asyncTransportExecutor.read(template.asyncTransportExecutor());
      remoteCommandsExecutor.read(template.remoteCommandsExecutor());
      evictionScheduledExecutor.read(template.evictionScheduledExecutor());
      globalJmxStatistics.read(template.globalJmxStatistics());
      replicationQueueScheduledExecutor.read(template.replicationQueueScheduledExecutor());
      security.read(template.security());
      serialization.read(template.serialization());
      shutdown.read(template.shutdown());
      transport.read(template.transport());
      site.read(template.sites());
      totalOrderExecutor.read(template.totalOrderExecutor());
      return this;
   }

   public static GlobalConfigurationBuilder defaultClusteredBuilder() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder
            .transport()
               .defaultTransport()
            .asyncTransportExecutor()
               .addProperty("threadNamePrefix", "asyncTransportThread");

      return builder;
   }

   @Override
   public String toString() {
      return "GlobalConfigurationBuilder [cl=" + cl + ", transport=" + transport + ", globalJmxStatistics="
            + globalJmxStatistics + ", serialization=" + serialization + ", asyncTransportExecutor="
            + asyncTransportExecutor + ", asyncListenerExecutor=" + asyncListenerExecutor + ", persistenceExecutor="
            + persistenceExecutor + ", remoteCommandsExecutor=" + remoteCommandsExecutor + ", totalOrderExecutor="
            + totalOrderExecutor + ", evictionScheduledExecutor=" + evictionScheduledExecutor
            + ", replicationQueueScheduledExecutor=" + replicationQueueScheduledExecutor + ", security=" + security
            + ", shutdown=" + shutdown + ", modules=" + modules + ", site=" + site + "]";
   }
}
