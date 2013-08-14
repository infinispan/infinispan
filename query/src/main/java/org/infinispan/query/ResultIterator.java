package org.infinispan.query;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Iterates over query results
 * <p/>
 *
 * @author Marko Luksa
 */
public interface ResultIterator extends Iterator<Object>, Closeable {

   /**
    * This method must be called on your iterator once you have finished so that any local
    * or remote resources can be freed up.
    */
   void close();
}
