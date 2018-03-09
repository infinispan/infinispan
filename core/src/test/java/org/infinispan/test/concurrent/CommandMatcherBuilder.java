package org.infinispan.test.concurrent;

import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.remoting.transport.Address;

/**
 * Builds {@link CommandMatcher}s.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class CommandMatcherBuilder<T extends ReplicableCommand> {
   private final Class<T>[] commandClasses;
   private String cacheName;
   private Address origin;
   private Object key;
   private long withFlags = 0;
   private long withoutFlags = 0;
   private int matchCount = -1;

   public CommandMatcherBuilder(Class<T>... commandClasses) {
      this.commandClasses = commandClasses;
   }

   public CommandMatcher build() {
      if (matchCount < 0) {
         return buildInternal();
      } else {
         return new MatchCountMatcher(buildInternal(), matchCount);
      }
   }

   private CommandMatcher buildInternal() {
      if (Stream.of(commandClasses).allMatch(CacheRpcCommand.class::isAssignableFrom)) {
         return new DefaultCommandMatcher(commandClasses, cacheName, origin);
      } else if (Stream.of(commandClasses).allMatch(DataCommand.class::isAssignableFrom)) {
         return new DefaultCommandMatcher(commandClasses, key, withFlags, withoutFlags);
      } else {
         return new DefaultCommandMatcher(commandClasses);
      }
   }

   public CommandMatcherBuilder withCache(Cache cache) {
      return withCache(cache.getName());
   }

   public CommandMatcherBuilder withCache(String cacheName) {
      this.cacheName = cacheName;
      return this;
   }

   /**
    * Note that a {@code null} origin means any origin, including local. If you need to match only local
    * commands, use {@link #localOnly()}.
    */
   public CommandMatcherBuilder withOrigin(Address origin) {
      this.origin = origin;
      return this;
   }

   public CommandMatcherBuilder localOnly() {
      this.origin = DefaultCommandMatcher.LOCAL_ORIGIN_PLACEHOLDER;
      return this;
   }

   public CommandMatcherBuilder remoteOnly() {
      this.origin = DefaultCommandMatcher.ANY_REMOTE_PLACEHOLDER;
      return this;
   }

   public CommandMatcherBuilder withKey(Object key) {
      this.key = key;
      return this;
   }

   public CommandMatcherBuilder withFlags(long flags) {
      this.withFlags = flags;
      return this;
   }

   public CommandMatcherBuilder withoutFlags(long flags) {
      this.withoutFlags = flags;
      return this;
   }

   /**
    * Accept only the {@code nth} invocation that matches <b>all</b> the other conditions.
    *
    * <p>The default, {@code matchCount = -1}, matches all invocations.
    * Use {@code matchCount >= 0} to match only one invocation, e.g. {@code matchCount = 0} matches the first invocation.
    */
   public CommandMatcherBuilder matchCount(int matchCount) {
      this.matchCount = matchCount;
      return this;
   }
}
