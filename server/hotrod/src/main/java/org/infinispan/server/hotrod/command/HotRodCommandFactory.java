package org.infinispan.server.hotrod.command;

import static java.lang.String.format;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.server.hotrod.command.tx.ForwardCommitCommand;
import org.infinispan.server.hotrod.command.tx.ForwardRollbackCommand;
import org.infinispan.util.ByteString;

/**
 * A {@link ModuleCommandFactory} that builds {@link CacheRpcCommand} used by this module.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class HotRodCommandFactory implements ModuleCommandFactory {
   @Override
   public Map<Byte, Class<? extends ReplicableCommand>> getModuleCommands() {
      Map<Byte, Class<? extends ReplicableCommand>> moduleCommands = new HashMap<>();
      moduleCommands.put(Ids.FORWARD_COMMIT, ForwardCommitCommand.class);
      moduleCommands.put(Ids.FORWARD_ROLLBACK, ForwardRollbackCommand.class);
      return moduleCommands;
   }

   @Override
   public ReplicableCommand fromStream(byte commandId) {
      return null;
   }

   @Override
   public CacheRpcCommand fromStream(byte commandId, ByteString cacheName) {
      switch (commandId) {
         case Ids.FORWARD_COMMIT:
            return new ForwardCommitCommand(cacheName);
         case Ids.FORWARD_ROLLBACK:
            return new ForwardRollbackCommand(cacheName);
         default:
            throw new IllegalArgumentException(format("Not registered to handle command id %s", commandId));
      }
   }
}
