package org.infinispan.test.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

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
   private final int matchCount;
   private final AtomicInteger matches = new AtomicInteger();

   public DefaultInvocationMatcher(String methodName) {
      this(methodName, null, -1, null);
   }

   public DefaultInvocationMatcher(String methodName, Matcher instanceMatcher, int matchCount, Matcher... argumentMatchers) {
      this.methodName = methodName;
      this.instanceMatcher = instanceMatcher;
      this.argumentMatchers = argumentMatchers;
      this.matchCount = matchCount;
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
      if (matchCount >= 0 && matches.getAndIncrement() != matchCount) {
         return false;
      }
      return true;
   }
}
