package org.infinispan.commons.jdkspecific;

public class CallerId {
   private static final StackWalker WALKER = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);

   public static Class<?> getCallerClass(int n) {
      return WALKER.walk(s ->
            s.map(StackWalker.StackFrame::getDeclaringClass).skip(n).findFirst().orElse(null));
   }

   public static String getCallerMethodName(int n) {
      return WALKER.walk(s ->
            s.map(StackWalker.StackFrame::getMethodName).skip(n).findFirst().orElse(null));
   }
}
