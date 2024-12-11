package org.infinispan.commands.module;

/**
 * Module command extensions. To use this hook, you would need to implement this interface and take the necessary steps
 * to make it discoverable by the {@link java.util.ServiceLoader} mechanism.
 *
 * @author Galder Zamarreño
 * @since 5.1
 * @deprecated since 15.1. This will class will be removed in 16.0 when commands will be marshalled via Protostream
 * <a href="https://github.com/infinispan/infinispan/issues/13247">#13247</a>
 */
@Deprecated(since = "15.1", forRemoval = true)
public interface ModuleCommandExtensions {

   ModuleCommandFactory getModuleCommandFactory();
}
