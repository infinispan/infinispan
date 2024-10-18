package org.infinispan.container.offheap;

import org.infinispan.commons.spi.OffHeapMemory;

/**
 * Accessors for the fields of a native LRU list node.
 *
 * @since 9.1
 */
class OffHeapLruNode {
   private static final OffHeapMemory MEMORY = org.infinispan.commons.jdkspecific.OffHeapMemory.getInstance();

   private static final int ADDRESS_SIZE = 8;

   private static final int PREVIOUS_NODE_OFFSET = 0;
   private static final int NEXT_NODE_OFFSET = PREVIOUS_NODE_OFFSET + ADDRESS_SIZE;

   private OffHeapLruNode() {
   }

   static long getNext(long lruNodeAddress) {
      return MEMORY.getLong(lruNodeAddress, NEXT_NODE_OFFSET);
   }

   static void setNext(long lruNodeAddress, long nextAddress) {
      MEMORY.putLong(lruNodeAddress, NEXT_NODE_OFFSET, nextAddress);
   }

   static long getPrevious(long lruNodeAddress) {
      return MEMORY.getLong(lruNodeAddress, PREVIOUS_NODE_OFFSET);
   }

   static void setPrevious(long lruNodeAddress, long previousAddress) {
      MEMORY.putLong(lruNodeAddress, PREVIOUS_NODE_OFFSET, previousAddress);
   }

   static String debugString(long address) {
      return String.format("0x%016x <-- entry 0x%016x --> 0x%016x", OffHeapLruNode.getPrevious(address), address,
            OffHeapLruNode.getNext(address));
   }
}
