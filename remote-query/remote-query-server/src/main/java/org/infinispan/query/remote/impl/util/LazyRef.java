package org.infinispan.query.remote.impl.util;

import java.util.function.Supplier;

/**
 * @since 9.2
 */
public class LazyRef<R> implements Supplier<R> {

   private final Supplier<R> supplier;
   private R supplied;
   private volatile boolean available;

   public LazyRef(Supplier<R> supplier) {
      this.supplier = supplier;
   }

   @Override
   public R get() {
      if (!available) {
         synchronized (this) {
            if (!available) {
               supplied = supplier.get();
               available = true;
            }
         }
      }
      return supplied;
   }
}
