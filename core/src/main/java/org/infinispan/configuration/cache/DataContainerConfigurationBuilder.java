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
package org.infinispan.configuration.cache;

import java.util.Properties;

import org.infinispan.container.DataContainer;
import org.infinispan.util.TypedProperties;

/**
 * Controls the data container for the cache.
 * 
 * @author pmuir
 *
 */
public class DataContainerConfigurationBuilder extends AbstractConfigurationChildBuilder<DataContainerConfiguration> {

   // TODO provide a deafult here
   private DataContainer dataContainer;
   private Properties properties = new Properties();
   
   DataContainerConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
   }
   
   /**
    * Specify the data container in use
    * @param dataContainer
    * @return
    */
   public DataContainerConfigurationBuilder dataContainer(DataContainer dataContainer) {
      this.dataContainer = dataContainer;
      return this;
   }
   
   /**
    * Add key/value property pair to this data container configuration
    *
    * @param key   property key
    * @param value property value
    * @return previous value if exists, null otherwise
    */
   public DataContainerConfigurationBuilder addProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
   }

   /**
    * Set key/value properties to this {@link DataContainer} configuration
    *
    * @param props Properties
    * @return this ExecutorFactoryConfig
    */
   public DataContainerConfigurationBuilder withProperties(Properties props) {
      this.properties = props;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   DataContainerConfiguration create() {
      return new DataContainerConfiguration(dataContainer, TypedProperties.toTypedProperties(properties));
   }
   
   @Override
   public DataContainerConfigurationBuilder read(DataContainerConfiguration template) {
      this.dataContainer = template.dataContainer();
      this.properties = template.properties();
      
      return this;
   }

}
