package org.infinispan.test.concurrent;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.remoting.transport.Address;

import java.util.concurrent.atomic.AtomicInteger;

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

   private final Class<? extends ReplicableCommand> commandClass;
   private final String cacheName;
   private final Address origin;
   private final Object key;

   private final AtomicInteger actualMatchCount = new AtomicInteger(0);

   public DefaultCommandMatcher(Class<? extends ReplicableCommand> commandClass) {
      this(commandClass, null, null, null);
   }

   public DefaultCommandMatcher(Class<? extends CacheRpcCommand> commandClass, String cacheName, Address origin) {
      this(commandClass, cacheName, origin, null);
   }

   public DefaultCommandMatcher(Class<? extends DataCommand> commandClass, Object key) {
      this(commandClass, null, null, key);
   }

   DefaultCommandMatcher(Class<? extends ReplicableCommand> commandClass, String cacheName, Address origin, Object key) {
      this.commandClass = commandClass;
      this.cacheName = cacheName;
      this.origin = origin;
      this.key = key;
   }

   @Override
   public boolean accept(ReplicableCommand command) {
      if (commandClass != null && !commandClass.equals(command.getClass()))
         return false;

      if (cacheName != null && !cacheName.equals(((CacheRpcCommand) command).getCacheName())) {
         return false;
      }

      if (origin != null && !addressMatches((CacheRpcCommand) command))
         return false;

      if (key != null && !key.equals(((DataCommand) command).getKey()))
         return false;

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
