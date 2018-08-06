package org.infinispan.container.offheap;

/**
 * Calculates an offset for a given object/hashCode. This is useful to project many entries into smaller sub blocks
 * as determined by the offset returned.
 * <p>
 * This interface is currently only for use with off heap block determination.
 * @author wburns
 * @since 9.4
 */
@FunctionalInterface
interface OffsetCalculator {
   default int calculateOffset(Object obj) {
      return calculateOffsetUsingHashCode(obj.hashCode());
   }

   int calculateOffsetUsingHashCode(int offset);
}
