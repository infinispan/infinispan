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
package org.infinispan.client.hotrod.configuration;

import java.util.Properties;

import org.infinispan.client.hotrod.impl.async.DefaultAsyncExecutorFactory;
import org.infinispan.configuration.Builder;
import org.infinispan.executors.ExecutorFactory;
import org.infinispan.util.TypedProperties;
import org.infinispan.util.Util;

/**
 * Configures executor factory.
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class ExecutorFactoryConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<ExecutorFactoryConfiguration> {

   private Class<? extends ExecutorFactory> factoryClass = DefaultAsyncExecutorFactory.class;
   private ExecutorFactory factory;
   private Properties properties;
   private final ConfigurationBuilder builder;

   ExecutorFactoryConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.builder = builder;
      this.properties = new Properties();
   }

   /**
    * Specify factory class for executor
    *
    * @param factory
    *           clazz
    * @return this ExecutorFactoryConfig
    */
   public ExecutorFactoryConfigurationBuilder factoryClass(Class<? extends ExecutorFactory> factoryClass) {
      this.factoryClass = factoryClass;
      return this;
   }

   public ExecutorFactoryConfigurationBuilder factoryClass(String factoryClass) {
      this.factoryClass = Util.loadClass(factoryClass, builder.classLoader());
      return this;
   }

   /**
    * Specify factory class for executor
    *
    * @param factory
    *           clazz
    * @return this ExecutorFactoryConfig
    */
   public ExecutorFactoryConfigurationBuilder factory(ExecutorFactory factory) {
      this.factory = factory;
      return this;
   }

   /**
    * Add key/value property pair to this executor factory configuration
    *
    * @param key
    *           property key
    * @param value
    *           property value
    * @return previous value if exists, null otherwise
    */
   public ExecutorFactoryConfigurationBuilder addExecutorProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
   }

   /**
    * Set key/value properties to this executor factory configuration
    *
    * @param props
    *           Properties
    * @return this ExecutorFactoryConfig
    */
   public ExecutorFactoryConfigurationBuilder withExecutorProperties(Properties props) {
      this.properties = props;
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public ExecutorFactoryConfiguration create() {
      if (factory != null)
         return new ExecutorFactoryConfiguration(factory, TypedProperties.toTypedProperties(properties));
      else
         return new ExecutorFactoryConfiguration(factoryClass, TypedProperties.toTypedProperties(properties));
   }

   @Override
   public ExecutorFactoryConfigurationBuilder read(ExecutorFactoryConfiguration template) {
      this.factory = template.factory();
      this.factoryClass = template.factoryClass();
      this.properties = template.properties();

      return this;
   }

   @Override
   public String toString() {
      return "ExecutorFactoryConfigurationBuilder [factoryClass=" + factoryClass + ", factory=" + factory + ", properties=" + properties + "]";
   }
}
