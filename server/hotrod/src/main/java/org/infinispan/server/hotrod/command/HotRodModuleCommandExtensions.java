package org.infinispan.server.hotrod.command;

import org.infinispan.commands.module.ModuleCommandExtensions;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commands.remote.CacheRpcCommand;

/**
 * It register the {@link HotRodCommandFactory} and {@link HotRodCommandInitializer} to handle the {@link
 * CacheRpcCommand} used by this module.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class HotRodModuleCommandExtensions implements ModuleCommandExtensions {

   @Override
   public ModuleCommandFactory getModuleCommandFactory() {
      return new HotRodCommandFactory();
   }

   @Override
   public ModuleCommandInitializer getModuleCommandInitializer() {
      return new HotRodCommandInitializer();
   }
}
