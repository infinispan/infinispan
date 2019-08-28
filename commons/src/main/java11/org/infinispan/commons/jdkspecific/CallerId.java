package org.infinispan.commons.jdkspecific;

public class CallerId {
   static private StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);

   public static Class<?> getCallerClass(int n) {
      return walker.walk(s ->
            s.map(StackWalker.StackFrame::getDeclaringClass).skip(n).findFirst().orElse(null));
   }
}
