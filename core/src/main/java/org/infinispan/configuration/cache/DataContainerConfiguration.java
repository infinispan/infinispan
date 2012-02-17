package org.infinispan.configuration.cache;

import org.infinispan.configuration.AbstractTypedPropertiesConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.util.TypedProperties;

/**
 * Controls the data container for the cache.
 * 
 * @author pmuir
 *
 */
public class DataContainerConfiguration extends AbstractTypedPropertiesConfiguration {

   private final DataContainer dataContainer;

   DataContainerConfiguration(DataContainer dataContainer, TypedProperties properties) {
      super(properties);
      this.dataContainer = dataContainer;
   }
   
   /**
    * Data container implementation in use
    * @return
    */
   public DataContainer dataContainer() {
      return dataContainer;
   }

   @Override
   public String toString() {
      return "DataContainerConfiguration{" +
            "dataContainer=" + dataContainer +
            '}';
   }

}
