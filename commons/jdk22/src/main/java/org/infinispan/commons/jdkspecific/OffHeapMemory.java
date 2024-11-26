package org.infinispan.commons.jdkspecific;

import org.infinispan.commons.jdk22.UnsafeMemoryAddressOffHeapMemory;

/**
 * Class used to retrieve an instance of {@link org.infinispan.commons.spi.OffHeapMemory} based on the current
 * running version of the JVM.
 */
public class OffHeapMemory {
   public static org.infinispan.commons.spi.OffHeapMemory getInstance() {
      return UnsafeMemoryAddressOffHeapMemory.getInstance();
   }
}
