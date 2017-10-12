package org.infinispan.container.offheap;

/**
 * Accessors for the fields of a native LRU list node.
 *
 * @since 9.1
 */
class OffHeapLruNode {
   private static final OffHeapMemory MEMORY = OffHeapMemory.INSTANCE;

   private static final int ADDRESS_SIZE = 8;
   private static final int HASHCODE_SIZE = 4;

   private static final int HASH_ENTRY_OFFSET = 0;
   private static final int PREVIOUS_NODE_OFFSET = HASH_ENTRY_OFFSET + ADDRESS_SIZE;
   private static final int NEXT_NODE_OFFSET = PREVIOUS_NODE_OFFSET + ADDRESS_SIZE;
   private static final int HASHCODE_OFFSET = NEXT_NODE_OFFSET + ADDRESS_SIZE;
   private static final int SIZE = HASHCODE_OFFSET + HASHCODE_SIZE;

   private OffHeapLruNode() {
   }

   static int getSize() {
      return SIZE;
   }

   static long getEntry(long lruNodeAddress) {
      return MEMORY.getLong(lruNodeAddress, HASH_ENTRY_OFFSET);
   }

   static void setEntry(long lruNodeAddress, long entryAddress) {
      MEMORY.putLong(lruNodeAddress, HASH_ENTRY_OFFSET, entryAddress);
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

   static int getHashCode(long lruNodeAddress) {
      return MEMORY.getInt(lruNodeAddress, HASHCODE_OFFSET);
   }

   static void setHashCode(long lruNodeAddress, int hashCode) {
      MEMORY.putInt(lruNodeAddress, HASHCODE_OFFSET, hashCode);
   }

   static String debugString(long address) {
      return String.format("0x%016x <-- 0x%016x (entry 0x%016x, hash 0x%08x) --> 0x%016x", OffHeapLruNode.getPrevious(address), address, OffHeapLruNode
            .getEntry(address), OffHeapLruNode.getHashCode(address), OffHeapLruNode.getNext(address));
   }
}
