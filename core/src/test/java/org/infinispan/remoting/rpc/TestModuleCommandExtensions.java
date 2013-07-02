package org.infinispan.remoting.rpc;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ExtendedModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandExtensions;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commands.remote.CacheRpcCommand;

import java.util.HashMap;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
public class TestModuleCommandExtensions implements ModuleCommandExtensions {

   @Override
   public ExtendedModuleCommandFactory getModuleCommandFactory() {
      return new ExtendedModuleCommandFactory() {
         @Override
         public Map<Byte, Class<? extends ReplicableCommand>> getModuleCommands() {
            Map<Byte, Class<? extends ReplicableCommand>> map = new HashMap<Byte, Class<? extends ReplicableCommand>>(2);
            map.put(CustomReplicableCommand.COMMAND_ID, CustomReplicableCommand.class);
            map.put(CustomCacheRpcCommand.COMMAND_ID, CustomCacheRpcCommand.class);
            map.put(SleepingCacheRpcCommand.COMMAND_ID, SleepingCacheRpcCommand.class);
            return map;
         }

         @Override
         public ReplicableCommand fromStream(byte commandId, Object[] args) {
            ReplicableCommand c;
            switch (commandId) {
               case CustomReplicableCommand.COMMAND_ID:
                  c = new CustomReplicableCommand();
                  break;
               default:
                  throw new IllegalArgumentException("Not registered to handle command id " + commandId);
            }
            c.setParameters(commandId, args);
            return c;
         }

         @Override
         public CacheRpcCommand fromStream(byte commandId, Object[] args, String cacheName) {
            CacheRpcCommand c;
            switch (commandId) {
               case CustomCacheRpcCommand.COMMAND_ID:
                  c = new CustomCacheRpcCommand(cacheName);
                  break;
               case SleepingCacheRpcCommand.COMMAND_ID:
                  c = new SleepingCacheRpcCommand(cacheName);
                  break;
               default:
                  throw new IllegalArgumentException("Not registered to handle command id " + commandId);
            }
            c.setParameters(commandId, args);
            return c;
         }
      };
   }

   @Override
   public ModuleCommandInitializer getModuleCommandInitializer() {
      return new ModuleCommandInitializer() {
         @Override
         public void initializeReplicableCommand(ReplicableCommand c, boolean isRemote) {
            // nothing to do here
         }
      };
   }
}
