package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.DeadlockDetectionConfiguration.ENABLED;
import static org.infinispan.configuration.cache.DeadlockDetectionConfiguration.SPIN_DURATION;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;
/**
 * Configures deadlock detection.
 */
public class DeadlockDetectionConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<DeadlockDetectionConfiguration> {

   private final AttributeSet attributes;

   DeadlockDetectionConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = DeadlockDetectionConfiguration.attributeDefinitionSet();
   }

   /**
    * Time period that determines how often is lock acquisition attempted within maximum time
    * allowed to acquire a particular lock
    */
   public DeadlockDetectionConfigurationBuilder spinDuration(long l) {
      attributes.attribute(SPIN_DURATION).set(l);
      return this;
   }

   /**
    * Time period that determines how often is lock acquisition attempted within maximum time
    * allowed to acquire a particular lock
    */
   public DeadlockDetectionConfigurationBuilder spinDuration(long l, TimeUnit unit) {
      return spinDuration(unit.toMillis(l));
   }

   /**
    * Enable deadlock detection
    */
   public DeadlockDetectionConfigurationBuilder enable() {
      attributes.attribute(ENABLED).set(true);
      return this;
   }

   /**
    * Disable deadlock detection
    */
   public DeadlockDetectionConfigurationBuilder disable() {
      attributes.attribute(ENABLED).set(false);
      return this;
   }

   /**
    * Enable or disable deadlock detection
    */
   public DeadlockDetectionConfigurationBuilder enabled(boolean enabled) {
      attributes.attribute(ENABLED).set(enabled);
      return this;
   }

   @Override
   public
   void validate() {
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public
   DeadlockDetectionConfiguration create() {
      return new DeadlockDetectionConfiguration(attributes.protect());
   }

   @Override
   public DeadlockDetectionConfigurationBuilder read(DeadlockDetectionConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public String toString() {
      return "DeadlockDetectionConfigurationBuilder [attributes=" + attributes + "]";
   }
}
