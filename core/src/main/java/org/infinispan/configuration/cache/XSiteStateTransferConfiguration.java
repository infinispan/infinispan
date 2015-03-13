package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

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

   static final AttributeDefinition<Integer> CHUNKSIZE = AttributeDefinition.builder("chunkSize", DEFAULT_CHUNK_SIZE).immutable().build();
   static final AttributeDefinition<Long> TIMEOUT = AttributeDefinition.builder("timeout", DEFAULT_TIMEOUT).build();
   static final AttributeDefinition<Integer> MAXRETRIES = AttributeDefinition.builder("maxRetries", DEFAULT_MAX_RETRIES).build();
   static final AttributeDefinition<Long> WAITTIME = AttributeDefinition.builder("waitTime", DEFAULT_WAIT_TIME).build();
   static AttributeSet attributeSet() {
      return new AttributeSet(XSiteStateTransferConfiguration.class, CHUNKSIZE, TIMEOUT, MAXRETRIES, WAITTIME);
   }

   private final AttributeSet attributes;

   public XSiteStateTransferConfiguration(AttributeSet attributes) {
      attributes.checkProtection();
      this.attributes = attributes;
   }

   public int chunkSize() {
      return this.attributes.attribute(CHUNKSIZE).asInteger();
   }

   public long timeout() {
      return this.attributes.attribute(TIMEOUT).asLong();
   }

   public int maxRetries() {
      return this.attributes.attribute(MAXRETRIES).asInteger();
   }

   public long waitTime() {
      return this.attributes.attribute(WAITTIME).asLong();
   }

   @Override
   public boolean equals(Object o) {
      XSiteStateTransferConfiguration other = (XSiteStateTransferConfiguration) o;
      return attributes.equals(other.attributes());
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return this.getClass().getSimpleName() + attributes;
   }

   AttributeSet attributes() {
      return attributes;
   }
}
