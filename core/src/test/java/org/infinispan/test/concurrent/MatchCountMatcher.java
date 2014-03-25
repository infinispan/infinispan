package org.infinispan.test.concurrent;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.remoting.transport.Address;

import java.util.concurrent.atomic.AtomicInteger;

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

   private final AtomicInteger actualMatchCount = new AtomicInteger(0);

   MatchCountMatcher(CommandMatcher matcher, int matchCount) {
      this.matcher = matcher;
      this.matchCount = matchCount;
   }

   @Override
   public boolean accept(ReplicableCommand command) {
      if (!matcher.accept(command))
         return false;

      // Only increment the counter if all the other conditions are met.
      if (matchCount >= 0 && actualMatchCount.getAndIncrement() != matchCount) {
         return false;
      }

      return true;
   }
}
