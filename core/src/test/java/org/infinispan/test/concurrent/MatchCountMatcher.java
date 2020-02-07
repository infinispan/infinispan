package org.infinispan.test.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commands.ReplicableCommand;

/**
 * {@link org.infinispan.test.concurrent.CommandMatcher} implementation that can match a single invocation (e.g. the
 * 2nd invocation that matches the other conditions).
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class MatchCountMatcher implements CommandMatcher {
   private final CommandMatcher matcher;
   private final int matchCount;

   private final AtomicInteger parentMatchCount = new AtomicInteger(0);

   /**
    * @param matcher Parent matcher
    * @param matchCount Index of invocation to match, e.g. {@code matchCount = 0} matches the first invocation.
    */
   MatchCountMatcher(CommandMatcher matcher, int matchCount) {
      if (matchCount < 0)
         throw new IllegalArgumentException("matchCount must be positive");

      this.matcher = matcher;
      this.matchCount = matchCount;
   }

   @Override
   public boolean accept(ReplicableCommand command) {
      if (!matcher.accept(command))
         return false;

      // Only increment the counter if all the other conditions are met.
      return parentMatchCount.getAndIncrement() == matchCount;
   }
}
