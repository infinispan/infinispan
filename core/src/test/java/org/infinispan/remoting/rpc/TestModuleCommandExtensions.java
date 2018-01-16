package org.infinispan.remoting.rpc;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ModuleCommandExtensions;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.ByteString;
import org.kohsuke.MetaInfServices;

/**
 * @author anistor@redhat.com
 * @since 5.3
 */
@MetaInfServices
@SuppressWarnings("unused")
public class TestModuleCommandExtensions implements ModuleCommandExtensions {

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
         @Inject
         public void injectDependencies(Cache cache) {
            // test that everything works when we inject the cache, see ISPN-5957
         }

         @Override
         public void initializeReplicableCommand(ReplicableCommand c, boolean isRemote) {
            // nothing to do here
         }
      };
   }
}
