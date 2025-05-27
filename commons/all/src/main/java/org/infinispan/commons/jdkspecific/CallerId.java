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

   public static String getStackTrace() {
      return StackWalker.getInstance()
            .walk(s -> {
               StringBuilder sb = new StringBuilder();
               // Skip our call
               s.skip(1)
                     .forEach(frame -> {
                        sb.append("\tat ").append(frame.getClassName())
                              .append(".").append(frame.getMethodName());
                        if (frame.getFileName() != null) {
                           sb.append("(").append(frame.getFileName());
                           if (frame.getLineNumber() >= 0) {
                              sb.append(":").append(frame.getLineNumber());
                           }
                           sb.append(")");
                        }
                        sb.append("\n");
                     });
               return sb.toString();
            });
   }
}
