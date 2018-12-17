package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;
/**
 * Configures deadlock detection.
 *
 * @deprecated Since 9.0, deadlock detection is always disabled.
 */
@Deprecated
public class DeadlockDetectionConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<DeadlockDetectionConfiguration>, ConfigurationBuilderInfo {

   private final AttributeSet attributes;

   DeadlockDetectionConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = DeadlockDetectionConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return DeadlockDetectionConfiguration.ELEMENT_DEFINITION;
   }

   /**
    * Time period that determines how often is lock acquisition attempted within maximum time
    * allowed to acquire a particular lock
    *
    * @deprecated Since 9.0, deadlock detection is always disabled.
    */
   @Deprecated
   public DeadlockDetectionConfigurationBuilder spinDuration(long l) {
      return this;
   }

   /**
    * Time period that determines how often is lock acquisition attempted within maximum time
    * allowed to acquire a particular lock
    *
    * @deprecated Since 9.0, deadlock detection is always disabled.
    */
   @Deprecated
   public DeadlockDetectionConfigurationBuilder spinDuration(long l, TimeUnit unit) {
      return spinDuration(unit.toMillis(l));
   }

   /**
    * Enable deadlock detection
    *
    * @deprecated Since 9.0, deadlock detection is always disabled.
    */
   @Deprecated
   public DeadlockDetectionConfigurationBuilder enable() {
      return this;
   }

   /**
    * Disable deadlock detection
    *
    * @deprecated Since 9.0, deadlock detection is always disabled.
    */
   @Deprecated
   public DeadlockDetectionConfigurationBuilder disable() {
      return this;
   }

   /**
    * Enable or disable deadlock detection
    *
    * @deprecated Since 9.0, deadlock detection is always disabled.
    */
   @Deprecated
   public DeadlockDetectionConfigurationBuilder enabled(boolean enabled) {
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
