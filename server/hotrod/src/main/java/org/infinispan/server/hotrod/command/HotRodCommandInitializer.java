package org.infinispan.server.hotrod.command;

import static java.lang.String.format;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.command.tx.ForwardCommitCommand;
import org.infinispan.server.hotrod.command.tx.ForwardRollbackCommand;

/**
 * A {@link ModuleCommandInitializer} implementation to initialize the {@link CacheRpcCommand} used by this module.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class HotRodCommandInitializer implements ModuleCommandInitializer {

   private EmbeddedCacheManager cacheManager;

   @Inject
   public void inject(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   public void initializeReplicableCommand(ReplicableCommand c, boolean isRemote) {
      final int commandId = c.getCommandId();
      switch (commandId) {
         case Ids.FORWARD_COMMIT:
            ((ForwardCommitCommand) c).inject(cacheManager);
            break;
         case Ids.FORWARD_ROLLBACK:
            ((ForwardRollbackCommand) c).inject(cacheManager);
            break;
         default:
            throw new IllegalArgumentException(format("Not registered to handle command id %s", commandId));
      }
   }
}
