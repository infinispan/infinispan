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

   @Override
   public TransportConfigurationBuilder transport() {
      return globalConfig.transport();
   }

   @Override
   public GlobalJmxStatisticsConfigurationBuilder globalJmxStatistics() {
      globalConfig.globalJmxStatistics().enable();
      return globalConfig.globalJmxStatistics();
   }

   @Override
   public SerializationConfigurationBuilder serialization() {
      return globalConfig.serialization();
   }

   @Override
   public ExecutorFactoryConfigurationBuilder asyncListenerExecutor() {

      return globalConfig.asyncListenerExecutor();
   }

   @Override
   public ExecutorFactoryConfigurationBuilder asyncTransportExecutor() {
      return globalConfig.asyncTransportExecutor();
   }

   @Override
   public ScheduledExecutorFactoryConfigurationBuilder evictionScheduledExecutor() {
      return globalConfig.evictionScheduledExecutor();
   }

   @Override
   public ScheduledExecutorFactoryConfigurationBuilder replicationQueueScheduledExecutor() {
      return globalConfig.replicationQueueScheduledExecutor();
   }

   @Override
   public ShutdownConfigurationBuilder shutdown() {
      return globalConfig.shutdown();
   }

   @Override
   public SitesConfigurationBuilder sites() {
      return globalConfig.sites();
   }

   @Override
   public GlobalConfiguration build() {
      return globalConfig.build();
   }

   abstract void validate();

   abstract T create();

   protected abstract GlobalConfigurationChildBuilder read(T template);

}