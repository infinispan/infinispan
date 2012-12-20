/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.configuration.global;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.infinispan.config.ConfigurationException;
import org.infinispan.configuration.Builder;
import org.infinispan.configuration.BuiltBy;
import static java.util.Arrays.asList;

public class GlobalConfigurationBuilder implements GlobalConfigurationChildBuilder {

   private ClassLoader cl;
   private final TransportConfigurationBuilder transport;
   private final GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics;
   private final SerializationConfigurationBuilder serialization;
   private final ExecutorFactoryConfigurationBuilder asyncTransportExecutor;
   private final ExecutorFactoryConfigurationBuilder asyncListenerExecutor;
   private final ScheduledExecutorFactoryConfigurationBuilder evictionScheduledExecutor;
   private final ScheduledExecutorFactoryConfigurationBuilder replicationQueueScheduledExecutor;
   private final ShutdownConfigurationBuilder shutdown;
   private final List<Builder<?>> modules = new ArrayList<Builder<?>>();
   private final SiteConfigurationBuilder site;

   public GlobalConfigurationBuilder() {
      this.cl = Thread.currentThread().getContextClassLoader();
      this.transport = new TransportConfigurationBuilder(this);
      this.globalJmxStatistics = new GlobalJmxStatisticsConfigurationBuilder(this);
      this.serialization = new SerializationConfigurationBuilder(this);
      this.asyncListenerExecutor = new ExecutorFactoryConfigurationBuilder(this);
      this.asyncTransportExecutor = new ExecutorFactoryConfigurationBuilder(this);
      this.evictionScheduledExecutor = new ScheduledExecutorFactoryConfigurationBuilder(this);
      this.replicationQueueScheduledExecutor = new ScheduledExecutorFactoryConfigurationBuilder(this);
      this.shutdown = new ShutdownConfigurationBuilder(this);
      this.site = new SiteConfigurationBuilder(this);
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
         throw new ConfigurationException("Could not instantiate module configuration builder '" + klass.getName() + "'", e);
      }
   }

   @SuppressWarnings("unchecked")
   public void validate() {
      for (AbstractGlobalConfigurationBuilder<?> validatable : asList(asyncListenerExecutor, asyncTransportExecutor,
            evictionScheduledExecutor, replicationQueueScheduledExecutor, globalJmxStatistics, transport,
            serialization, shutdown, site)) {
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
            evictionScheduledExecutor.create(),
            replicationQueueScheduledExecutor.create(),
            globalJmxStatistics.create(),
            transport.create(),
            serialization.create(),
            shutdown.create(),
            modulesConfig,
            site.create(),
            cl
            );
   }

   public GlobalConfigurationBuilder read(GlobalConfiguration template) {
      this.cl = template.classLoader();

      for (Object c : template.modules().values()) {
         BuiltBy builtBy = c.getClass().getAnnotation(BuiltBy.class);
         Builder<Object> builder = (Builder<Object>) this.addModule(builtBy.value());
         builder.read(c);
      }

      asyncListenerExecutor.read(template.asyncListenerExecutor());
      asyncTransportExecutor.read(template.asyncTransportExecutor());
      evictionScheduledExecutor.read(template.evictionScheduledExecutor());
      globalJmxStatistics.read(template.globalJmxStatistics());
      replicationQueueScheduledExecutor.read(template.replicationQueueScheduledExecutor());
      serialization.read(template.serialization());
      shutdown.read(template.shutdown());
      transport.read(template.transport());
      site.read(template.sites());
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
            ", cl=" + cl +
            ", transport=" + transport +
            ", globalJmxStatistics=" + globalJmxStatistics +
            ", serialization=" + serialization +
            ", asyncTransportExecutor=" + asyncTransportExecutor +
            ", evictionScheduledExecutor=" + evictionScheduledExecutor +
            ", replicationQueueScheduledExecutor=" + replicationQueueScheduledExecutor +
            ", shutdown=" + shutdown +
            ", site=" + site +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      GlobalConfigurationBuilder that = (GlobalConfigurationBuilder) o;

      if (asyncListenerExecutor != null ? !asyncListenerExecutor.equals(that.asyncListenerExecutor) : that.asyncListenerExecutor != null)
         return false;
      if (asyncTransportExecutor != null ? !asyncTransportExecutor.equals(that.asyncTransportExecutor) : that.asyncTransportExecutor != null)
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

      return true;
   }

   @Override
   public int hashCode() {
      int result = cl != null ? cl.hashCode() : 0;
      result = 31 * result + (transport != null ? transport.hashCode() : 0);
      result = 31 * result + (globalJmxStatistics != null ? globalJmxStatistics.hashCode() : 0);
      result = 31 * result + (serialization != null ? serialization.hashCode() : 0);
      result = 31 * result + (asyncTransportExecutor != null ? asyncTransportExecutor.hashCode() : 0);
      result = 31 * result + (asyncListenerExecutor != null ? asyncListenerExecutor.hashCode() : 0);
      result = 31 * result + (evictionScheduledExecutor != null ? evictionScheduledExecutor.hashCode() : 0);
      result = 31 * result + (replicationQueueScheduledExecutor != null ? replicationQueueScheduledExecutor.hashCode() : 0);
      result = 31 * result + (shutdown != null ? shutdown.hashCode() : 0);
      result = 31 * result + (site != null ? site.hashCode() : 0);
      return result;
   }

}