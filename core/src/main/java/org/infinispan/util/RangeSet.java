package org.infinispan.util;

/**
 * Read-only set representing all the integers from {@code 0} to {@code size - 1} (inclusive).
 *
 * @author Dan Berindei
 * @since 9.0
 * @deprecated Use {@link org.infinispan.commons.util.RangeSet} instead
 */
public class RangeSet extends org.infinispan.commons.util.RangeSet {
   public RangeSet(int size) {
      super(size);
   }
}
