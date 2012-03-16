/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.global;

abstract class AbstractGlobalConfigurationBuilder<T> implements GlobalConfigurationChildBuilder {
   
   private final GlobalConfigurationBuilder globalConfig;
   
   protected AbstractGlobalConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      this.globalConfig = globalConfig;
   }
   
   protected GlobalConfigurationBuilder getGlobalConfig() {
      return globalConfig;
   }

   public TransportConfigurationBuilder transport() {
      return globalConfig.transport();
   }

   public GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics() {
      globalConfig.globalJmxStatistics().enable();
      return globalConfig.globalJmxStatistics();
   }

   public SerializationConfigurationBuilder serialization() {
      return globalConfig.serialization();
   }

   public ExecutorFactoryConfigurationBuilder asyncListenerExecutor() {
      
      return globalConfig.asyncListenerExecutor();
   }

   public ExecutorFactoryConfigurationBuilder asyncTransportExecutor() {
      return globalConfig.asyncTransportExecutor();
   }
   
   public ScheduledExecutorFactoryConfigurationBuilder evictionScheduledExecutor() {
      return globalConfig.evictionScheduledExecutor();
   }

   public ScheduledExecutorFactoryConfigurationBuilder replicationQueueScheduledExecutor() {
      return globalConfig.replicationQueueScheduledExecutor();
   }

   public ShutdownConfigurationBuilder shutdown() {
      return globalConfig.shutdown();
   }

   public GlobalConfiguration build() {
      return globalConfig.build();
   }
   
   abstract void validate();
   
   abstract T create();
   
   abstract GlobalConfigurationChildBuilder read(T template);
   
}