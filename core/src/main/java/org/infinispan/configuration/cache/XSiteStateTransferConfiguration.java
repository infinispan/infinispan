package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configuration needed for State Transfer between different sites.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class XSiteStateTransferConfiguration {
   public static final int DEFAULT_CHUNK_SIZE = 512;
   public static final long DEFAULT_TIMEOUT = TimeUnit.MINUTES.toMillis(20);
   public static final int DEFAULT_MAX_RETRIES = 30;
   public static final long DEFAULT_WAIT_TIME = TimeUnit.SECONDS.toMillis(2);

   public static final AttributeDefinition<Integer> CHUNK_SIZE = AttributeDefinition.builder("chunkSize", DEFAULT_CHUNK_SIZE).immutable().build();
   public static final AttributeDefinition<Long> TIMEOUT = AttributeDefinition.builder("timeout", DEFAULT_TIMEOUT).build();
   public static final AttributeDefinition<Integer> MAX_RETRIES = AttributeDefinition.builder("maxRetries", DEFAULT_MAX_RETRIES).build();
   public static final AttributeDefinition<Long> WAIT_TIME = AttributeDefinition.builder("waitTime", DEFAULT_WAIT_TIME).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(XSiteStateTransferConfiguration.class, CHUNK_SIZE, TIMEOUT, MAX_RETRIES, WAIT_TIME);
   }

   private final Attribute<Integer> chunkSize;
   private final Attribute<Long> timeout;
   private final Attribute<Integer> maxRetries;
   private final Attribute<Long> waitTime;
   private final AttributeSet attributes;

   public XSiteStateTransferConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      chunkSize = attributes.attribute(CHUNK_SIZE);
      timeout = attributes.attribute(TIMEOUT);
      maxRetries = attributes.attribute(MAX_RETRIES);
      waitTime = attributes.attribute(WAIT_TIME);
   }

   public int chunkSize() {
      return chunkSize.get();
   }

   public long timeout() {
      return timeout.get();
   }

   public int maxRetries() {
      return maxRetries.get();
   }

   public long waitTime() {
      return waitTime.get();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      XSiteStateTransferConfiguration other = (XSiteStateTransferConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

   @Override
   public String toString() {
      return "XSiteStateTransferConfiguration [attributes=" + attributes + "]";
   }

   public AttributeSet attributes() {
      return attributes;
   }
}
