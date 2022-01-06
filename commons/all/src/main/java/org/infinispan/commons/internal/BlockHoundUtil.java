package org.infinispan.commons.internal;

public class BlockHoundUtil {
   /**
    * Signal to BlockHound that a method must not be called from a non-blocking thread.
    *
    * <p>Helpful when the method only blocks some of the time, and it is hard to force it to block
    * in all the scenarios that use it.</p>
    */
   public static void pretendBlock() {
      // Do nothing
   }
}
