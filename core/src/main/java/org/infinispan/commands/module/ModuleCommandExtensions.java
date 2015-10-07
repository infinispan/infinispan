package org.infinispan.commands.module;

/**
 * Module command extensions
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface ModuleCommandExtensions {

   ModuleCommandFactory getModuleCommandFactory();

   ModuleCommandInitializer getModuleCommandInitializer();

}
