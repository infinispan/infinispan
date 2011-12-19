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

import org.infinispan.remoting.transport.jgroups.JGroupsTransport;

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
         .clearTransport()
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
   
   public TransportConfigurationBuilder transport() {
      return transport;
   }

   /**
    * This method allows configuration of the global, or cache manager level,
    * jmx statistics. When this method is called, it automatically enables
    * global jmx statistics. So, if you want it to be disabled, make sure you call
    * {@link org.infinispan.config.FluentGlobalConfiguration.GlobalJmxStatisticsConfig#disable()}
    */
   public GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics() {
      globalJmxStatistics.enable();
      return globalJmxStatistics;
   }

   public SerializationConfigurationBuilder serialization() {
      return serialization;
   }

   public ExecutorFactoryConfigurationBuilder asyncTransportExecutor() {
      return asyncTransportExecutor;
   }

   public ExecutorFactoryConfigurationBuilder asyncListenerExecutor() {
      return asyncListenerExecutor;
   }

   public ScheduledExecutorFactoryConfigurationBuilder evictionScheduledExecutor() {
      return evictionScheduledExecutor;
   }

   public ScheduledExecutorFactoryConfigurationBuilder replicationQueueScheduledExecutor() {
      return replicationQueueScheduledExecutor;
   }

   public ShutdownConfigurationBuilder shutdown() {
      return shutdown;
   }

   public GlobalConfiguration build() {
      return new GlobalConfiguration(
            asyncListenerExecutor.create(), 
            asyncTransportExecutor.create(), 
            evictionScheduledExecutor.create(), 
            replicationQueueScheduledExecutor.create(), 
            globalJmxStatistics.create(),
            transport.create(),
            serialization.create(), 
            shutdown.create(),
            cl
            );
   }
   
   public GlobalConfigurationBuilder read(GlobalConfiguration template) {
      this.cl = template.classLoader();
      
      asyncListenerExecutor.read(template.asyncListenerExecutor());
      asyncTransportExecutor.read(template.asyncTransportExecutor());
      evictionScheduledExecutor.read(template.evictionScheduledExecutor());
      globalJmxStatistics.read(template.globalJmxStatistics());
      replicationQueueScheduledExecutor.read(template.replicationQueueScheduledExecutor());
      serialization.read(template.serialization());
      shutdown.read(template.shutdown());
      transport.read(template.transport());
      
      return this;
   }

   public static GlobalConfigurationBuilder defaultClusteredBuilder() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder
            .transport()
               .transport(new JGroupsTransport())
            .asyncTransportExecutor()
               .addProperty("threadNamePrefix", "asyncTransportThread");

      return builder;
   }
}