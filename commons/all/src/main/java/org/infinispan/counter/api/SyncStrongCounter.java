package org.infinispan.counter.api;

/**
 * A synchronous {@link StrongCounter}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public interface SyncStrongCounter {

   /**
    * @see StrongCounter#incrementAndGet()
    */
   default long incrementAndGet() {
      return addAndGet(1);
   }

   /**
    * @see StrongCounter#decrementAndGet()
    */
   default long decrementAndGet() {
      return addAndGet(-1);
   }

   /**
    * @see StrongCounter#addAndGet(long)
    */
   long addAndGet(long delta);

   /**
    * @see StrongCounter#reset()
    */
   void reset();

   /**
    * @see StrongCounter#decrementAndGet()
    */
   long getValue();

   /**
    * @see StrongCounter#compareAndSet(long, long)
    */
   default boolean compareAndSet(long expect, long update) {
      return compareAndSwap(expect, update) == expect;
   }

   /**
    * @see StrongCounter#compareAndSwap(long, long)
    */
   long compareAndSwap(long expect, long update);

   /**
    * @see StrongCounter#getName()
    */
   String getName();

   /**
    * @see StrongCounter#getConfiguration()
    */
   CounterConfiguration getConfiguration();

   /**
    * @see StrongCounter#remove()
    */
   void remove();
}
