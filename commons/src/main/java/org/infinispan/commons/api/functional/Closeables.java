package org.infinispan.commons.api.functional;

import java.util.Spliterator;

// TODO: Remove once Will's branch closeable spliterator is in.

/**
 * @since 8.0
 */
public class Closeables {

   private Closeables() {
      // Cannot be instantiated, it's just a holder class
   }

   /**
    * Auto closeable spliterator. Being able to close an spliterator is useful
    * for situations where the iterator might consume considerable resources,
    * e.g. in a distributed environment. Hence, being able to close it allows
    * for resources to be reclaimed if the iterator won't be iterated any more.
    */
   public interface CloseableSpliterator<T> extends Spliterator<T>, AutoCloseable {
      @Override void close();
   }

}
