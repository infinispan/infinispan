package org.infinispan.test.concurrent;

import org.hamcrest.Matcher;

/**
 * Default {@link InvocationMatcher} implementation.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class DefaultInvocationMatcher implements InvocationMatcher {
   private final String methodName;
   private final Matcher instanceMatcher;
   private final Matcher[] argumentMatchers;

   public DefaultInvocationMatcher(String methodName) {
      this(methodName, null, null);
   }

   public DefaultInvocationMatcher(String methodName, Matcher instanceMatcher, Matcher... argumentMatchers) {
      this.methodName = methodName;
      this.instanceMatcher = instanceMatcher;
      this.argumentMatchers = argumentMatchers;
   }

   @Override
   public boolean accept(Object instance, String methodName, Object[] arguments) {
      if (!methodName.equals(this.methodName))
         return false;
      if (instanceMatcher != null && !instanceMatcher.matches(instance))
         return false;
      if (argumentMatchers != null) {
         for (int i = 0; i < argumentMatchers.length; i++) {
            if (argumentMatchers[i] != null && !argumentMatchers[i].matches(arguments[i]))
               return false;
         }
      }
      return true;
   }
}
