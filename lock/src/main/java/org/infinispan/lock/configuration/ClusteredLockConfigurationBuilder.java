package org.infinispan.lock.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * {@link org.infinispan.lock.api.ClusteredLock} configuration builder.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.4
 */
public class ClusteredLockConfigurationBuilder implements Builder<ClusteredLockConfiguration> {

   private final AttributeSet attributes = ClusteredLockConfiguration.attributeDefinitionSet();

   @Override
   public void validate() {
      attributes.attributes().forEach(Attribute::validate);
   }

   @Override
   public ClusteredLockConfiguration create() {
      return new ClusteredLockConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(ClusteredLockConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   public ClusteredLockConfigurationBuilder name(String name) {
      attributes.attribute(ClusteredLockConfiguration.NAME).set(name);
      return this;
   }
}
