package org.infinispan.api.mutiny;

import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public interface MutinyWeakCounter {
   /**
    * Returns the name of this counter
    *
    * @return the name of this counter
    */
   String name();

   /**
    * Return the container of this counter
    * @return
    */
   MutinyContainer container();

   /**
    * Returns the current value of this counter
    * @return
    */
   Uni<Long> value();

   default Uni<Void> increment() {
      return add(1);
   }

   default Uni<Void> decrement() {
      return add(-1);
   }

   Uni<Void> add(long delta);
}
