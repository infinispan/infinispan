package org.infinispan.remoting.rpc;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ModuleCommandExtensions;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.ByteString;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
public final class TestModuleCommandExtensions implements ModuleCommandExtensions {

   @Override
   public ModuleCommandFactory getModuleCommandFactory() {
      return new ModuleCommandFactory() {
         @Override
         public Map<Byte, Class<? extends ReplicableCommand>> getModuleCommands() {
            Map<Byte, Class<? extends ReplicableCommand>> map = new HashMap<>(2);
            map.put(CustomReplicableCommand.COMMAND_ID, CustomReplicableCommand.class);
            map.put(CustomCacheRpcCommand.COMMAND_ID, CustomCacheRpcCommand.class);
            map.put(SleepingCacheRpcCommand.COMMAND_ID, SleepingCacheRpcCommand.class);
            return map;
         }

         @Override
         public ReplicableCommand fromStream(byte commandId) {
            ReplicableCommand c;
            switch (commandId) {
               case CustomReplicableCommand.COMMAND_ID:
                  c = new CustomReplicableCommand();
                  break;
               default:
                  throw new IllegalArgumentException("Not registered to handle command id " + commandId);
            }
            return c;
         }

         @Override
         public CacheRpcCommand fromStream(byte commandId, ByteString cacheName) {
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
            return c;
         }
      };
   }

   @Override
   public ModuleCommandInitializer getModuleCommandInitializer() {
      return new ModuleCommandInitializer() {
         @Inject EmbeddedCacheManager cacheManager;

         @Override
         public void initializeReplicableCommand(ReplicableCommand c, boolean isRemote) {
            // nothing to do here
         }
      };
   }
}
