package org.infinispan.commons.jdkspecific;

import sun.reflect.Reflection;

/**
 * JDK 8 implementation which uses sun.reflect.Reflection
 */
public class CallerId {
   private static final boolean hasGetCallerClass;
   private static final int callerOffset;
   private static final LocalSecurityManager SECURITY_MANAGER;

   static {
      boolean result = false;
      int offset = 1;
      try {
         result = Reflection.getCallerClass(1) == CallerId.class || Reflection.getCallerClass(2) == CallerId.class;
         offset = Reflection.getCallerClass(1) == Reflection.class ? 2 : 1;
      } catch (Throwable ignored) {
      }
      hasGetCallerClass = result;
      callerOffset = offset;
      if (!hasGetCallerClass) {
         SECURITY_MANAGER = new LocalSecurityManager();
      } else {
         SECURITY_MANAGER = null;
      }
   }

   private static class LocalSecurityManager extends SecurityManager {
      public Class<?>[] getClasses() {
         return this.getClassContext();
      }
   }

   public static Class<?> getCallerClass(int n) {
      if (hasGetCallerClass) {
         return Reflection.getCallerClass(n + callerOffset);
      } else {
         return SECURITY_MANAGER.getClasses()[n + callerOffset];
      }
   }
}
