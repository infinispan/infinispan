package org.infinispan.query.impl;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Initializes query module remote commands.
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 * @since 5.1
 */
final class CommandInitializer implements ModuleCommandInitializer {

   private EmbeddedCacheManager cacheManager;

   public void setCacheManager(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   @Override
   public void initializeReplicableCommand(ReplicableCommand c, boolean isRemote) {
      // all commands returned by our CommandFactory.getModuleCommands() extend CustomQueryCommand
      CustomQueryCommand queryCommand = (CustomQueryCommand) c;
      queryCommand.setCacheManager(cacheManager);
   }
}
