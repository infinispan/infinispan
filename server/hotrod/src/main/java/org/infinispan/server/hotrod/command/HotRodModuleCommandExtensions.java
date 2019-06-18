package org.infinispan.server.hotrod.command;

import org.infinispan.commands.module.ModuleCommandExtensions;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.remote.CacheRpcCommand;

/**
 * It register the {@link HotRodCommandFactory} to handle the {@link CacheRpcCommand} used by this module.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public final class HotRodModuleCommandExtensions implements ModuleCommandExtensions {

   @Override
   public ModuleCommandFactory getModuleCommandFactory() {
      return new HotRodCommandFactory();
   }
}
