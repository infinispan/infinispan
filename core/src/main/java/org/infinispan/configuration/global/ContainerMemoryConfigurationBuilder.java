package org.infinispan.configuration.global;

import static org.infinispan.util.logging.Log.CONFIG;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * Configures the container memory which allows for a shared memory space used by multiple caches that can be bounded
 * so when the limit is surpassed an entry is evicted to ensure memory does not grow too much. The memory container
 * supports both count based (number of entries) and size based (how much approximate memory in bytes) eviction methods.
 * @since 16.1
 * @author William Burns
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

   /**
    * Defines the maximum size of the container memory.
    *
    * @param size The size of the memory container in bytes.
    *             This can be a number or a string ending with KB, MB, GB, TB, PB (e.g. "10MB").
    * @return <code>this</code>, for method chaining
    */
   public ContainerMemoryConfigurationBuilder maxSize(String size) {
      attributes.attribute(ContainerMemoryConfiguration.MAX_SIZE).set(size);
      return this;
   }

   /**
    * The currently configured max size.
    *
    * @return The size as a string.
    */
   public String maxSize() {
      return attributes.attribute(ContainerMemoryConfiguration.MAX_SIZE).get();
   }

   /**
    * Defines the maximum number of entries for the container memory.
    *
    * @param count The maximum number of entries.
    * @return <code>this</code>, for method chaining
    */
   public ContainerMemoryConfigurationBuilder maxCount(long count) {
      attributes.attribute(ContainerMemoryConfiguration.MAX_COUNT).set(count);
      return this;
   }

   /**
    * The currently configured max count.
    *
    * @return The maximum number of entries.
    */
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
      if (isSizeBounded() && isCountBounded()) {
         throw CONFIG.cannotProvideBothSizeAndCount();
      }
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
