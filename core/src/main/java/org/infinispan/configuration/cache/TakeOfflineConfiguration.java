package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class TakeOfflineConfiguration extends ConfigurationElement<TakeOfflineConfiguration> {
   public static final AttributeDefinition<Integer> AFTER_FAILURES = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.TAKE_BACKUP_OFFLINE_AFTER_FAILURES, 0).immutable().build();
   public static final AttributeDefinition<Long> MIN_TIME_TO_WAIT = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.TAKE_BACKUP_OFFLINE_MIN_WAIT, 0l).immutable().build();
   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TakeOfflineConfiguration.class, AFTER_FAILURES, MIN_TIME_TO_WAIT);
   }

   private final Attribute<Integer> afterFailures;
   private final Attribute<Long> minTimeToWait;

   public TakeOfflineConfiguration(AttributeSet attributes) {
      super(Element.TAKE_OFFLINE, attributes);
      afterFailures = attributes.attribute(AFTER_FAILURES);
      minTimeToWait = attributes.attribute(MIN_TIME_TO_WAIT);
   }

   /**
    * @see TakeOfflineConfigurationBuilder#afterFailures(int)
    */
   public int afterFailures() {
      return afterFailures.get();
   }

   /**
    * @see TakeOfflineConfigurationBuilder#minTimeToWait(long)
    */
   public long minTimeToWait() {
      return minTimeToWait.get();
   }

   public boolean enabled() {
      return afterFailures() > 0 || minTimeToWait() > 0;
   }
}
