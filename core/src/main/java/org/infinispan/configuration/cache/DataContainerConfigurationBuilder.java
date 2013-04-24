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

import org.infinispan.compat.TypeConverter;
import org.infinispan.configuration.Builder;
import org.infinispan.container.DataContainer;
import org.infinispan.util.AnyEquivalence;
import org.infinispan.util.Equivalence;
import org.infinispan.util.TypedProperties;

/**
 * Controls the data container for the cache.
 *
 * @author pmuir
 *
 */
public class DataContainerConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<DataContainerConfiguration> {

   // No default here. DataContainerFactory figures out default.
   private DataContainer dataContainer;
   private Equivalence keyEquivalence = AnyEquivalence.OBJECT;
   private Equivalence valueEquivalence = AnyEquivalence.OBJECT;
   // TODO: What are properties used for? Is it just legacy?
   private Properties properties = new Properties();
   private TypeConverter typeConverter;

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

   /**
    * Set the {@link org.infinispan.util.Equivalence} instance to use to compare keys stored in
    * data container. {@link org.infinispan.util.Equivalence} implementations allow for custom
    * comparisons to be provided when the JDK, or external libraries, do
    * not provide adequate comparison implementations, i.e. arrays.
    *
    * @param keyEquivalence instance of {@link org.infinispan.util.Equivalence} used to compare
    *                     key types.
    * @return this configuration builder
    */
   public DataContainerConfigurationBuilder keyEquivalence(Equivalence keyEquivalence) {
      this.keyEquivalence = keyEquivalence;
      return this;
   }

   /**
    * Set the {@link org.infinispan.util.Equivalence} instance to use to compare values stored in
    * data container. {@link org.infinispan.util.Equivalence} implementations allow for custom
    * comparisons to be provided when the JDK, or external libraries, do
    * not provide adequate comparison implementations, i.e. arrays.
    *
    * @param valueEquivalence instance of {@link org.infinispan.util.Equivalence} used to compare
    *                       value types.
    * @return this configuration builder
    */
   public DataContainerConfigurationBuilder valueEquivalence(Equivalence valueEquivalence) {
      this.valueEquivalence = valueEquivalence;
      return this;
   }

   /**
    * Set the {@link TypeConverter} instance to use to convert cached key and
    * value instances into target key and value type instances. Type
    * converters are used to provide compatibility between the different
    * Infinispan endpoints (i.e. embedded, hot rod, ...etc).
    *
    * @param typeConverter instance of {@link TypeConverter} used to convert instances
    * @return this configuration builder
    */
   public DataContainerConfigurationBuilder typeConverter(TypeConverter typeConverter) {
      this.typeConverter = typeConverter;
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public DataContainerConfiguration create() {
      return new DataContainerConfiguration(dataContainer,
            TypedProperties.toTypedProperties(properties), keyEquivalence,
            valueEquivalence, typeConverter);
   }

   @Override
   public DataContainerConfigurationBuilder read(DataContainerConfiguration template) {
      this.dataContainer = template.dataContainer();
      this.properties = template.properties();
      this.keyEquivalence = template.keyEquivalence();
      this.valueEquivalence = template.valueEquivalence();
      this.typeConverter = template.typeConverter();

      return this;
   }

   @Override
   public String toString() {
      return "DataContainerConfigurationBuilder{" +
            "dataContainer=" + dataContainer +
            ", properties=" + properties +
            ", keyEquivalence=" + keyEquivalence +
            ", valueEquivalence=" + valueEquivalence +
            ", typeConverter=" + typeConverter +
            '}';
   }

}
