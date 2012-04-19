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

import java.util.Properties;

import org.infinispan.executors.DefaultScheduledExecutorFactory;
import org.infinispan.executors.ScheduledExecutorFactory;
import org.infinispan.util.TypedProperties;

/**
 * Configures executor factory.
 */
public class ScheduledExecutorFactoryConfigurationBuilder extends AbstractGlobalConfigurationBuilder<ScheduledExecutorFactoryConfiguration> {
   
   private ScheduledExecutorFactory factory = new DefaultScheduledExecutorFactory();
   private Properties properties;
   
   ScheduledExecutorFactoryConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      this.properties = new Properties();
   }
   
   /**
    * Specify factory class for executor
    * 
    * NOTE: Currently Infinispan will not use the object instance, but instead instantiate a new
    * instance of the class. Therefore, do not expect any state to survive, and provide a no-args
    * constructor to any instance. This will be resolved in Infinispan 5.2.0
    *
    * @param factory clazz
    * @return this ScheduledExecutorFactoryConfig
    */
   public ScheduledExecutorFactoryConfigurationBuilder factory(ScheduledExecutorFactory factory) {
      this.factory = factory;
      return this;
   }

   /**
    * Add key/value property pair to this executor factory configuration
    *
    * @param key   property key
    * @param value property value
    * @return previous value if exists, null otherwise
    */
   public ScheduledExecutorFactoryConfigurationBuilder addProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
   }

   /**
    * Set key/value properties to this executor factory configuration
    *
    * @param props Properties
    * @return this ScheduledExecutorFactoryConfig
    */
   public ScheduledExecutorFactoryConfigurationBuilder withProperties(Properties props) {
      this.properties = props;
      return this;
   }
   
   @Override
   void validate() {
      // No-op, no validation required
   } 
   
   @Override
   ScheduledExecutorFactoryConfiguration create() {
      return new ScheduledExecutorFactoryConfiguration(factory, TypedProperties.toTypedProperties(properties));
   }
   
   @Override
   public ScheduledExecutorFactoryConfigurationBuilder read(ScheduledExecutorFactoryConfiguration template) {
      this.factory = template.factory();
      this.properties = template.properties();
      
      return this;
   }

   @Override
   public String toString() {
      return "ScheduledExecutorFactoryConfigurationBuilder{" +
            "factory=" + factory +
            ", properties=" + properties +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ScheduledExecutorFactoryConfigurationBuilder that = (ScheduledExecutorFactoryConfigurationBuilder) o;

      if (factory != null ? !factory.equals(that.factory) : that.factory != null)
         return false;
      if (properties != null ? !properties.equals(that.properties) : that.properties != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = factory != null ? factory.hashCode() : 0;
      result = 31 * result + (properties != null ? properties.hashCode() : 0);
      return result;
   }

}