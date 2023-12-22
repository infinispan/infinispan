package org.infinispan.embedded.impl;

import java.util.Iterator;

import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.common.CloseableIterator;

public class EmbeddedUtil {

   public static <V> CloseableIterable<V> closeableIterable(Iterable<V> i) {
      Iterator<V> it = i.iterator();
      return () -> new CloseableIterator<>() {
         @Override
         public void close() {
            // no-op
         }

         @Override
         public boolean hasNext() {
            return it.hasNext();
         }

         @Override
         public V next() {
            return it.next();
         }
      };
   }
}
