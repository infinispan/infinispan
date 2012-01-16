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
