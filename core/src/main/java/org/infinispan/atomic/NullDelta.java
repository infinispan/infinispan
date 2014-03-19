package org.infinispan.atomic;

import org.infinispan.atomic.impl.AtomicHashMap;

/**
 * Represents no changes.
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @author Galder Zamarreño
 * @since 4.0
 */
public enum NullDelta implements Delta {
   INSTANCE;

   @Override
   public DeltaAware merge(DeltaAware other) {
      return (other != null && other instanceof AtomicHashMap) ? other : new AtomicHashMap();
   }
}