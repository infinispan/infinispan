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
      for (AbstractGlobalConfigurationBuilder<?> validatable : asList(asyncListenerExecutor, persistenceExecutor, asyncTransportExecutor,
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
         Builder<Object> builder = (Builder<Object>) this.addModule(builtBy.value());
         builder.read(c);
      }

      asyncListenerExecutor.read(template.asyncListenerExecutor());
      persistenceExecutor.read(template.asyncListenerExecutor());
      asyncTransportExecutor.read(template.asyncTransportExecutor());
      remoteCommandsExecutor.read(template.remoteCommandsExecutor());
      evictionScheduledExecutor.read(template.evictionScheduledExecutor());
      globalJmxStatistics.read(template.globalJmxStatistics());
      replicationQueueScheduledExecutor.read(template.replicationQueueScheduledExecutor());
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
      return "GlobalConfigurationBuilder{" +
            "asyncListenerExecutor=" + asyncListenerExecutor +
            "persistenceExecutor=" + persistenceExecutor +
            ", cl=" + cl +
            ", transport=" + transport +
            ", globalJmxStatistics=" + globalJmxStatistics +
            ", serialization=" + serialization +
            ", asyncTransportExecutor=" + asyncTransportExecutor +
            ", remoteCommandsExecutor=" + remoteCommandsExecutor +
            ", evictionScheduledExecutor=" + evictionScheduledExecutor +
            ", replicationQueueScheduledExecutor=" + replicationQueueScheduledExecutor +
            ", shutdown=" + shutdown +
            ", site=" + site +
            ", totalOrderExecutor=" + totalOrderExecutor +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      GlobalConfigurationBuilder that = (GlobalConfigurationBuilder) o;

      if (asyncListenerExecutor != null ? !asyncListenerExecutor.equals(that.asyncListenerExecutor) : that.asyncListenerExecutor != null)
         return false;
      if (persistenceExecutor != null ? !persistenceExecutor.equals(that.persistenceExecutor) : that.persistenceExecutor!= null)
         return false;
      if (asyncTransportExecutor != null ? !asyncTransportExecutor.equals(that.asyncTransportExecutor) : that.asyncTransportExecutor != null)
         return false;
      if (remoteCommandsExecutor != null ? !remoteCommandsExecutor.equals(that.remoteCommandsExecutor) : that.remoteCommandsExecutor != null)
         return false;
      if (cl != null ? !cl.equals(that.cl) : that.cl != null) return false;
      if (evictionScheduledExecutor != null ? !evictionScheduledExecutor.equals(that.evictionScheduledExecutor) : that.evictionScheduledExecutor != null)
         return false;
      if (globalJmxStatistics != null ? !globalJmxStatistics.equals(that.globalJmxStatistics) : that.globalJmxStatistics != null)
         return false;
      if (replicationQueueScheduledExecutor != null ? !replicationQueueScheduledExecutor.equals(that.replicationQueueScheduledExecutor) : that.replicationQueueScheduledExecutor != null)
         return false;
      if (serialization != null ? !serialization.equals(that.serialization) : that.serialization != null)
         return false;
      if (shutdown != null ? !shutdown.equals(that.shutdown) : that.shutdown != null)
         return false;
      if (site != null ? !site.equals(that.site) : that.site != null)
         return false;
      if (transport != null ? !transport.equals(that.transport) : that.transport != null)
         return false;

      return !(totalOrderExecutor != null ? !totalOrderExecutor.equals(that.totalOrderExecutor()) : that.totalOrderExecutor != null);
   }

   @Override
   public int hashCode() {
      int result = cl != null ? cl.hashCode() : 0;
      result = 31 * result + (transport != null ? transport.hashCode() : 0);
      result = 31 * result + (globalJmxStatistics != null ? globalJmxStatistics.hashCode() : 0);
      result = 31 * result + (serialization != null ? serialization.hashCode() : 0);
      result = 31 * result + (asyncTransportExecutor != null ? asyncTransportExecutor.hashCode() : 0);
      result = 31 * result + (asyncListenerExecutor != null ? asyncListenerExecutor.hashCode() : 0);
      result = 31 * result + (remoteCommandsExecutor != null ? remoteCommandsExecutor.hashCode() : 0);
      result = 31 * result + (evictionScheduledExecutor != null ? evictionScheduledExecutor.hashCode() : 0);
      result = 31 * result + (replicationQueueScheduledExecutor != null ? replicationQueueScheduledExecutor.hashCode() : 0);
      result = 31 * result + (shutdown != null ? shutdown.hashCode() : 0);
      result = 31 * result + (site != null ? site.hashCode() : 0);
      result = 31 * result + (totalOrderExecutor != null ? totalOrderExecutor().hashCode() : 0);
      return result;
   }

}
