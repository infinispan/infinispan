package org.infinispan.commons.jdkspecific;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.util.Util;

public class OffHeapMemory {
   private static final org.infinispan.commons.spi.OffHeapMemory INSTANCE = createInstance();

   public static org.infinispan.commons.spi.OffHeapMemory getInstance() {
      return INSTANCE;
   }

   private static org.infinispan.commons.spi.OffHeapMemory createInstance() {
      try {
         org.infinispan.commons.spi.OffHeapMemory instance = Util.getInstance("org.infinispan.commons.jdk21.UnsafeMemoryAddressOffHeapMemory", ThreadCreator.class.getClassLoader());
         Log.CONTAINER.infof("Foreign memory support enabled");
         return instance;
      } catch (Throwable t) {
         Log.CONTAINER.debugf("Could not initialize foreign memory, using Unsafe", t);
      }
      return UnsafeOffHeapMemory.INSTANCE;
   }
}
