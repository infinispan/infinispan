package org.infinispan.commons.util;

import java.util.function.Supplier;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 13.0
 **/
public class InstanceSupplier<T> implements Supplier<T> {
   private final T instance;

   public InstanceSupplier(T instance) {
      this.instance = instance;
   }

   @Override
   public T get() {
      return instance;
   }
}
