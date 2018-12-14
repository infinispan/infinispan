package org.infinispan.lock.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.lock.logging.Log;

/**
 * {@link org.infinispan.lock.api.ClusteredLock} configuration.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.4
 */
public class ClusteredLockConfiguration {

   private static final Log log = LogFactory.getLog(ClusteredLockConfiguration.class, Log.class);
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class)
         .xmlName("name")
         .validator(value -> {
            if (value == null) {
               throw log.missingName();
            }
         })
         .immutable()
         .build();

   final AttributeSet attributes;

   ClusteredLockConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
   }

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ClusteredLockConfiguration.class, NAME);
   }

   final AttributeSet attributes() {
      return attributes;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }
}
