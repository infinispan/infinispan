package org.infinispan.commands.module;

/**
 * Module command extensions. To use this hook, you would need to implement this interface and take the necessary steps
 * to make it discoverable by the {@link java.util.ServiceLoader} mechanism.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface ModuleCommandExtensions {

   ModuleCommandFactory getModuleCommandFactory();

   /**
    * @deprecated Since 10.0, Commands which require initialization should implement {@link
    * org.infinispan.commands.InitializableCommand}. This will be removed in next major version.
    */
   @Deprecated
   default ModuleCommandInitializer getModuleCommandInitializer() {
      return null;
   }
}
