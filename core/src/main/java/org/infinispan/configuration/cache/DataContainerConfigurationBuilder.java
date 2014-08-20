package org.infinispan.configuration.cache;

import java.util.Properties;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.equivalence.AnyEquivalence;
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

   // No default here. DataContainerFactory figures out default.
   private DataContainer dataContainer;
   private Equivalence keyEquivalence = AnyEquivalence.getInstance();
   private Equivalence valueEquivalence = AnyEquivalence.getInstance();
   // TODO: What are properties used for? Is it just legacy?
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
      this.keyEquivalence = keyEquivalence;
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
      this.valueEquivalence = valueEquivalence;
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
      return new DataContainerConfiguration(dataContainer,
            TypedProperties.toTypedProperties(properties), keyEquivalence,
            valueEquivalence);
   }

   @Override
   public DataContainerConfigurationBuilder read(DataContainerConfiguration template) {
      this.dataContainer = template.dataContainer();
      this.properties = template.properties();
      this.keyEquivalence = template.keyEquivalence();
      this.valueEquivalence = template.valueEquivalence();

      return this;
   }

   @Override
   public String toString() {
      return "DataContainerConfigurationBuilder{" +
            "dataContainer=" + dataContainer +
            ", properties=" + properties +
            ", keyEquivalence=" + keyEquivalence +
            ", valueEquivalence=" + valueEquivalence +
            '}';
   }
}
