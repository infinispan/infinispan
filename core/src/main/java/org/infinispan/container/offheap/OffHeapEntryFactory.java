package org.infinispan.container.offheap;

import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;

/**
 * Factory that can create {@link InternalCacheEntry} objects that use off-heap heap memory.  These are stored by
 * a long to symbolize the memory address.
 * @author wburns
 * @since 9.0
 */
public interface OffHeapEntryFactory {
   /**
    *
    * @param key
    * @param value
    * @param metadata
    * @return
    */
   long create(WrappedBytes key, WrappedBytes value, Metadata metadata);

   /**
    * Returns how many bytes in memory this address location uses assuming it is an {@link InternalCacheEntry}
    * @param address
    * @return
    */
   long determineSize(long address);

   /**
    *
    * @param address
    * @return
    */
   InternalCacheEntry<WrappedBytes, WrappedBytes> fromMemory(long address);

   /**
    * Returns whether the given key as bytes is the same key as the key stored in the entry for the given address.
    * @param address the address of the entry's key to check
    * @param wrappedBytes the key to check equality with
    * @return whether or not the keys are equal
    */
   boolean equalsKey(long address, WrappedBytes wrappedBytes);
}
