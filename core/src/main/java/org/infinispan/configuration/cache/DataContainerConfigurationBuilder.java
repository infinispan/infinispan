package org.infinispan.configuration.cache;

import static org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration.PROPERTIES;
import static org.infinispan.configuration.cache.DataContainerConfiguration.DATA_CONTAINER;
import static org.infinispan.configuration.cache.DataContainerConfiguration.KEY_EQUIVALENCE;
import static org.infinispan.configuration.cache.DataContainerConfiguration.VALUE_EQUIVALENCE;

import java.util.Properties;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.DataContainer;

/**
 * Controls the data container for the cache.
 *
 * @author pmuir
 *
 */
public class DataContainerConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<DataContainerConfiguration> {

   private AttributeSet attributes;

   DataContainerConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = DataContainerConfiguration.attributeDefinitionSet();
   }

   /**
    * Specify the data container in use
    * @param dataContainer
    * @return
    */
   public DataContainerConfigurationBuilder dataContainer(DataContainer dataContainer) {
      attributes.attribute(DATA_CONTAINER).set(dataContainer);
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
      TypedProperties properties = attributes.attribute(PROPERTIES).get();
      properties.put(key, value);
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(properties));
      return this;
   }

   /**
    * Set key/value properties to this {@link DataContainer} configuration
    *
    * @param props Properties
    * @return this ExecutorFactoryConfig
    */
   public DataContainerConfigurationBuilder withProperties(Properties props) {
      attributes.attribute(PROPERTIES).set(TypedProperties.toTypedProperties(props));
      return this;
   }

   /**
    * Set the {@link org.infinispan.commons.equivalence.Equivalence} instance to use to compare keys stored in
    * data container. {@link org.infinispan.commons.equivalence.Equivalence} implementations allow for custom
    * comparisons to be provided when the JDK, or external libraries, do
    * not provide adequate comparison implementations, i.e. arrays.
    *
    * @param keyEquivalence instance of {@link org.infinispan.commons.equivalence.Equivalence} used to compare
    *                     key types.
    * @return this configuration builder
    */
   public <K> DataContainerConfigurationBuilder keyEquivalence(Equivalence<K> keyEquivalence) {
      attributes.attribute(KEY_EQUIVALENCE).set(keyEquivalence);
      return this;
   }

   /**
    * Set the {@link org.infinispan.commons.equivalence.Equivalence} instance to use to compare values stored in
    * data container. {@link org.infinispan.commons.equivalence.Equivalence} implementations allow for custom
    * comparisons to be provided when the JDK, or external libraries, do
    * not provide adequate comparison implementations, i.e. arrays.
    *
    * @param valueEquivalence instance of {@link org.infinispan.commons.equivalence.Equivalence} used to compare
    *                       value types.
    * @return this configuration builder
    */
   public <V> DataContainerConfigurationBuilder valueEquivalence(Equivalence<V> valueEquivalence) {
      attributes.attribute(VALUE_EQUIVALENCE).set(valueEquivalence);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public DataContainerConfiguration create() {
      return new DataContainerConfiguration(attributes.protect());
   }

   @Override
   public DataContainerConfigurationBuilder read(DataContainerConfiguration template) {
      attributes.read(template.attributes());

      return this;
   }

   @Override
   public String toString() {
      return "DataContainerConfigurationBuilder [attributes=" + attributes + "]";
   }
}
