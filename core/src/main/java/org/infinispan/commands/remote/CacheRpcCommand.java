package org.infinispan.commands.remote;

import org.infinispan.commands.ReplicableCommand;

/**
 * The {@link org.infinispan.remoting.RpcManager} only replicates commands wrapped in a {@link CacheRpcCommand}.
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface CacheRpcCommand extends ReplicableCommand {

   /**
    * @return the name of the cache that produced this command.  This will also be the name of the cache this command is
    *         intended for.
    */
   String getCacheName();
}
