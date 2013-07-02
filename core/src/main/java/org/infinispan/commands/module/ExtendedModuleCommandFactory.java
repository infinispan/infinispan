package org.infinispan.commands.module;

import org.infinispan.commands.remote.CacheRpcCommand;

/**
 * Temporary workaround to avoid modifying {@link ModuleCommandFactory}
 * interface. This interface should be merged with {@link ModuleCommandFactory}
 * in 6.0.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public interface ExtendedModuleCommandFactory extends ModuleCommandFactory {

   /**
    * Construct and initialize a {@link CacheRpcCommand} based on the command
    * id and argument array passed in.
    *
    * @param commandId  command id to construct
    * @param args       array of arguments with which to initialize the {@link CacheRpcCommand}
    * @param cacheName  cache name at which command to be created is directed
    * @return           a {@link CacheRpcCommand}
    */
   CacheRpcCommand fromStream(byte commandId, Object[] args, String cacheName);

}
