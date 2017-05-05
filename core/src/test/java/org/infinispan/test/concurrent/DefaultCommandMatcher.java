package org.infinispan.test.concurrent;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.remoting.transport.Address;

/**
 * Generic {@link CommandMatcher} implementation that can use both {@link CacheRpcCommand} criteria (cache name, origin)
 * and {@link DataCommand} criteria (key).
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class DefaultCommandMatcher implements CommandMatcher {
   public static final Address LOCAL_ORIGIN_PLACEHOLDER = new AddressPlaceholder();
   public static final Address ANY_REMOTE_PLACEHOLDER = new AddressPlaceholder();

   private final Class<? extends ReplicableCommand>[] commandClasses;
   private final String cacheName;
   private final Address origin;
   private final Object key;
   private final long withFlags;
   private final long withoutFlags;

   private final AtomicInteger actualMatchCount = new AtomicInteger(0);

   public DefaultCommandMatcher(Class<? extends ReplicableCommand>... commandClasses) {
      this(commandClasses, null, null, null, 0, 0);
   }

   public DefaultCommandMatcher(Class<? extends ReplicableCommand>[] commandClass, String cacheName, Address origin) {
      this(commandClass, cacheName, origin, null, 0, 0);
   }

   public DefaultCommandMatcher(Class<? extends ReplicableCommand>[] commandClasses, Object key, long withFlags, long withoutFlags) {
      this(commandClasses, null, null, key, withFlags, withoutFlags);
   }

   DefaultCommandMatcher(Class<? extends ReplicableCommand>[] commandClasses, String cacheName, Address origin, Object key, long withFlags, long withoutFlags) {
      this.commandClasses = commandClasses;
      this.cacheName = cacheName;
      this.origin = origin;
      this.key = key;
      this.withFlags = withFlags;
      this.withoutFlags = withoutFlags;
   }

   @Override
   public boolean accept(ReplicableCommand command) {
      if (commandClasses != null && !Stream.of(commandClasses).anyMatch(command.getClass()::equals))
         return false;

      if (cacheName != null && !cacheName.equals(((CacheRpcCommand) command).getCacheName().toString())) {
         return false;
      }

      if (origin != null && !addressMatches((CacheRpcCommand) command))
         return false;

      if (key != null && !key.equals(((DataCommand) command).getKey()))
         return false;

      if (command instanceof FlagAffectedCommand) {
         long flags = ((FlagAffectedCommand) command).getFlagsBitSet();
         if ((flags & withFlags) != withFlags || (flags & withoutFlags) != 0) return false;
      }

      return true;
   }

   private boolean addressMatches(CacheRpcCommand command) {
      Address commandOrigin = command.getOrigin();
      if (origin == LOCAL_ORIGIN_PLACEHOLDER)
         return commandOrigin == null;
      else if (origin == ANY_REMOTE_PLACEHOLDER)
         return commandOrigin != null;
      else
         return !origin.equals(commandOrigin);
   }

   private static class AddressPlaceholder implements Address {
      @Override
      public int compareTo(Address o) {
         throw new UnsupportedOperationException("This address should never be part of a view");
      }
   }
}
