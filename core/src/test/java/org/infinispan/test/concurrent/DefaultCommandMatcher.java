package org.infinispan.test.concurrent;

import org.infinispan.commands.DataCommand;
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

   private final Class<? extends ReplicableCommand> commandClass;
   private final String cacheName;
   private final Address origin;
   private final Object key;

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
      if (commandClass != null && commandClass != command.getClass())
         return false;

      if (cacheName != null && !cacheName.equals(((CacheRpcCommand) command).getCacheName().toString())) {
         return false;
      }

      if (origin != null && origin.equals(((CacheRpcCommand) command).getOrigin()))
         return false;

      if (key != null && !key.equals(((DataCommand) command).getKey()))
         return false;

      return true;
   }
}
