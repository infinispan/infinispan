package org.infinispan.jcache;

import java.io.Closeable;
import java.util.Set;

import org.infinispan.jcache.logging.Log;

class Closeables {

   private static final Log log = Log.getLog(Closeables.class);

   private Closeables() {
   }

   static void close(Object o) {
      if (o instanceof Closeable) {
         Closeable c = (Closeable) o;
         try {
            c.close();
         } catch (Exception e) {
            log.errorClosingCloseable(c, e);
         }
      }
   }

   static void close(Set<?> set) {
      set.stream()
         .filter(Closeable.class::isInstance)
         .map(Closeable.class::cast)
         .forEach(c -> {
            try {
               c.close();
            } catch (Exception e) {
               log.errorClosingCloseable(c, e);
            }
         });
   }

}
