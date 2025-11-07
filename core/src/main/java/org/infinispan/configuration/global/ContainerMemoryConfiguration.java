package org.infinispan.configuration.global;

import static org.infinispan.util.logging.Log.CONFIG;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.ByteQuantity;
import org.infinispan.configuration.parsing.Attribute;

public class ContainerMemoryConfiguration {
   public static final AttributeDefinition<String> MAX_SIZE = AttributeDefinition.builder(Attribute.MAX_SIZE, null, String.class).matcher((a1, a2) -> maxSizeToBytes(a1.get()) == maxSizeToBytes(a2.get())).build();
   public static final AttributeDefinition<Long> MAX_COUNT = AttributeDefinition.builder(Attribute.MAX_COUNT, -1L).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ContainerMemoryConfiguration.class, MAX_SIZE, MAX_COUNT);
   }

   protected static long maxSizeToBytes(String maxSizeStr) {
      return maxSizeStr != null ? ByteQuantity.parse(maxSizeStr) : -1;
   }

   private final AttributeSet attributes;

   ContainerMemoryConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   /**
    * @return The max size in bytes or -1 if not configured.
    */
   public long maxSizeBytes() {
      return maxSizeToBytes(maxSize());
   }

   public String maxSize() {
      return attributes.attribute(MAX_SIZE).get();
   }

   public void maxSize(String maxSize) {
      if (!isSizeBounded()) throw CONFIG.cannotChangeMaxSize();
      attributes.attribute(MAX_SIZE).set(maxSize);
   }

   public void maxSize(long maxSize) {
      maxSize(Long.toString(maxSize));
   }

   /**
    * @return the max number of entries in memory or -1 if not configured.
    */
   public long maxCount() {
      return attributes.attribute(MAX_COUNT).get();
   }

   public void maxCount(long maxCount) {
      if (!isCountBounded()) throw CONFIG.cannotChangeMaxCount();
      attributes.attribute(MAX_COUNT).set(maxCount);
   }

   private boolean isSizeBounded() {
      return maxSize() != null;
   }

   private boolean isCountBounded() {
      return maxCount() > 0;
   }

   public boolean isEvictionEnabled() {
      return maxSize() != null || maxCount() > 0;
   }
}
