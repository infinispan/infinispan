package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configures container memory.
 */
public class ContainerMemoryConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<ContainerMemoryConfiguration> {
   private final AttributeSet attributes;

   ContainerMemoryConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      this.attributes = ContainerMemoryConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public ContainerMemoryConfigurationBuilder maxSize(String size) {
      attributes.attribute(ContainerMemoryConfiguration.MAX_SIZE).set(size);
      return this;
   }

   public ContainerMemoryConfigurationBuilder maxSize(long l) {
      attributes.attribute(ContainerMemoryConfiguration.MAX_SIZE).set(Long.toString(l));
      return this;
   }

   public String maxSize() {
      return attributes.attribute(ContainerMemoryConfiguration.MAX_SIZE).get();
   }

   public ContainerMemoryConfigurationBuilder maxCount(long count) {
      attributes.attribute(ContainerMemoryConfiguration.MAX_COUNT).set(count);
      return this;
   }

   public long maxCount() {
      return attributes.attribute(ContainerMemoryConfiguration.MAX_COUNT).get();
   }

   boolean isSizeBounded() {
      return maxSize() != null;
   }

   boolean isCountBounded() {
      return maxCount() > 0;
   }

   @Override
   public void validate() {
      // No-op, no validation required
   }

   @Override
   public ContainerMemoryConfiguration create() {
      return new ContainerMemoryConfiguration(attributes.protect());
   }

   @Override
   public ContainerMemoryConfigurationBuilder read(ContainerMemoryConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "ContainerMemoryConfigurationBuilder [attributes=" + attributes + "]";
   }

}
