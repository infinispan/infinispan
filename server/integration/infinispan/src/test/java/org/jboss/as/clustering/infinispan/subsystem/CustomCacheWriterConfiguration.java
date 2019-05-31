package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@ConfigurationFor(CustomCacheWriter.class)
public class CustomCacheWriterConfiguration extends AbstractStoreConfiguration {

   private String someProperty;
   private final String location;

   public CustomCacheWriterConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, String someProperty,
                                         String location) {
      super(attributes, async);
      this.someProperty = someProperty;
      this.location = location;
   }

   public String someProperty() {
      return someProperty;
   }

   public String getLocation() {
      return location;
   }
}
