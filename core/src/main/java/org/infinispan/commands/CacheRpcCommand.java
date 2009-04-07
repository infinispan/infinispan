package org.infinispan.commands;

/**
 * The RPCManager only replicates commands wrapped in an RPCCommand.  As a wrapper, an RPCCommand could contain a single
 * {@link org.infinispan.commands.ReplicableCommand} or a List of them.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheRpcCommand extends ReplicableCommand {

   /**
    * @return the name of the cache that produced this command.  This will also be the name of the cache this command is
    *         intended for.
    */
   String getCacheName();
}
