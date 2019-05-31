package org.jboss.as.clustering.infinispan.subsystem;

import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;

/**
 * @author Mircea Markus
 * @since 6.0
 */
@ConfigurationFor(CustomCacheLoader.class)
public class CustomCacheLoaderConfiguration extends AbstractStoreConfiguration {

   private String location;

   public CustomCacheLoaderConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, String location) {
      super(attributes, async);
      this.location = location;
   }

   public String location() {
      return location;
   }
}
