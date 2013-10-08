package org.infinispan.query.impl;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * Initializes query module remote commands
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 * @since 5.1
 */
public final class CommandInitializer implements ModuleCommandInitializer {

   private EmbeddedCacheManager cacheManager;

   public void setCacheManager(EmbeddedCacheManager cacheManager) {
	   this.cacheManager = cacheManager;
   }

   @Override
   public void initializeReplicableCommand(final ReplicableCommand c, final boolean isRemote) {
      //we don't waste cycles to check it's the correct type, as that would be a
      //critical error anyway: let it throw a ClassCastException.
      CustomQueryCommand queryCommand = (CustomQueryCommand) c;
      queryCommand.fetchExecutionContext(this);
   }

   public final EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }

}
