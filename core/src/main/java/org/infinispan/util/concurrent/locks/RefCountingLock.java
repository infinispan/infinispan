package org.infinispan.util.concurrent.locks;

import java.util.concurrent.locks.Lock;

/**
 * An extension of a JDK {@link Lock}, with support for maintaining a reference counter.
 *
 * @author Manik Surtani
 * @since 5.2
 */
public interface RefCountingLock extends Lock {
   /**
    * Increments reference counter for this lock
    * @return Updated value of the counter
    */
   int incrementRefCountAndGet();

   /**
    * Decrementes reference counter for this lock
    * @return Updated value of the counter
    */
   int decrementRefCountAndGet();
}
