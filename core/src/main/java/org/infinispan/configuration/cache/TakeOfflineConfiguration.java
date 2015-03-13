package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class TakeOfflineConfiguration {
   static final AttributeDefinition<Integer> AFTER_FAILURES = AttributeDefinition.builder("afterFailures", 0).immutable().build();
   static final AttributeDefinition<Long> MIN_TIME_TO_WAIT = AttributeDefinition.builder("minTimeToWait", 0l).immutable().build();
   static AttributeSet attributeSet() {
      return new AttributeSet(TakeOfflineConfiguration.class, AFTER_FAILURES, MIN_TIME_TO_WAIT);
   }
   private final AttributeSet attributes;

   public TakeOfflineConfiguration(AttributeSet attributes) {
      attributes.checkProtection();
      this.attributes = attributes;
   }

   /**
    * @see TakeOfflineConfigurationBuilder#afterFailures(int)
    */
   public int afterFailures() {
      return attributes.attribute(AFTER_FAILURES).asInteger();
   }

   /**
    * @see TakeOfflineConfigurationBuilder#minTimeToWait(long)
    */
   public long minTimeToWait() {
      return attributes.attribute(MIN_TIME_TO_WAIT).asLong();
   }

   public boolean enabled() {
      return afterFailures() > 0 || minTimeToWait() > 0;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      TakeOfflineConfiguration other = (TakeOfflineConfiguration) obj;
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
      return this.getClass().getSimpleName() + attributes;
   }
}
