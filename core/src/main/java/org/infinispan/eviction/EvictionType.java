package org.infinispan.eviction;

/**
 * Supported eviction type
 *
 * @author Tristan Tarrant
 * @since 8.0
 */
public enum EvictionType {
   COUNT(false, false),
   COUNT_EXCEPTION(false, true),
   MEMORY(true, false),
   MEMORY_EXCEPTION(true, true),
   ;

   private boolean memory;
   private boolean exception;

   EvictionType(boolean memory, boolean exception) {
      this.memory = memory;
      this.exception = exception;
   }

   public boolean isExceptionBased() {
      return exception;
   }

   public boolean isMemoryBased() {
      return memory;
   }
}
