package org.infinispan.test.concurrent;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.transport.Address;

/**
 * Builds {@link CommandMatcher}s.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class CommandMatcherBuilder {
   private final Class<? extends ReplicableCommand> commandClass;
   private String cacheName;
   private Address origin;
   private Object key;

   public CommandMatcherBuilder(Class<? extends ReplicableCommand> commandClass) {
      this.commandClass = commandClass;
   }

   public CommandMatcher build() {
      return new DefaultCommandMatcher(commandClass, cacheName, origin, key);
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
}
