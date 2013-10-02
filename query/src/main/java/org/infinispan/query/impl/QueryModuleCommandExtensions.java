package org.infinispan.query.impl;

import org.infinispan.commands.module.ExtendedModuleCommandFactory;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commands.module.ModuleCommandExtensions;

public class QueryModuleCommandExtensions implements ModuleCommandExtensions {

   @Override
   public ExtendedModuleCommandFactory getModuleCommandFactory() {
      return new CommandFactory();
   }

   @Override
   public ModuleCommandInitializer getModuleCommandInitializer() {
      return new CommandInitializer();
   }

}
