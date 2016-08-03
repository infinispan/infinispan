package org.infinispan.test.concurrent;

import org.infinispan.commands.ReplicableCommand;
import org.mockito.internal.invocation.*;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link InvocationMatcher} implementation that can match a single invocation (e.g. the
 * 2nd invocation that matches the other conditions).
 *
 * @since 9.0
 */
public class MatchCountInvocationMatcher implements InvocationMatcher {
   private final InvocationMatcher matcher;
   private final int matchCount;

   private final AtomicInteger actualMatchCount = new AtomicInteger(0);

   MatchCountInvocationMatcher(InvocationMatcher matcher, int matchCount) {
      this.matcher = matcher;
      this.matchCount = matchCount;
   }

   @Override
   public boolean accept(Object instance, String methodName, Object[] arguments) {
      if (!matcher.accept(instance, methodName, arguments))
         return false;

      // Only increment the counter if all the other conditions are met.
      if (matchCount >= 0 && actualMatchCount.getAndIncrement() != matchCount) {
         return false;
      }

      return true;
   }
}
