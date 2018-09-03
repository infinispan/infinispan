package org.infinispan.query;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Iterates over query results. Must be closed when done with it.
 *
 * @author Marko Luksa
 */
public interface ResultIterator<E> extends Iterator<E>, Closeable {

   /**
    * This method must be called on your iterator once you have finished so that any local or remote resources can be
    * freed up.
    */
   @Override
   void close();
}
