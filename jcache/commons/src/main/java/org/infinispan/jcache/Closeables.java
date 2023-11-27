package org.infinispan.jcache;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.jcache.logging.Log;

import java.io.Closeable;
import java.util.Set;

class Closeables {

   private static final Log log = LogFactory.getLog(Closeables.class, Log.class);

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
