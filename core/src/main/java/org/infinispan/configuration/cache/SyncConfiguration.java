package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * If configured all communications are synchronous, in that whenever a thread sends a message sent
 * over the wire, it blocks until it receives an acknowledgment from the recipient. SyncConfig is
 * mutually exclusive with the AsyncConfig.
 */
public class SyncConfiguration {

   public static final AttributeDefinition<Long> REPL_TIMEOUT = AttributeDefinition.builder("replTimeout", TimeUnit.SECONDS.toMillis(15)).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SyncConfiguration.class.getSimpleName(), REPL_TIMEOUT);
   }

   private final Attribute<Long> replTimeout;
   private final AttributeSet attributes;

   SyncConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      replTimeout = attributes.attribute(REPL_TIMEOUT);
   }

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public long replTimeout() {
      return replTimeout.get();
   }

   /**
    * This is the timeout used to wait for an acknowledgment when making a remote call, after which
    * the call is aborted and an exception is thrown.
    */
   public SyncConfiguration replTimeout(long l) {
      replTimeout.set(l);
      return this;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "SyncConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      SyncConfiguration other = (SyncConfiguration) obj;
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

}
