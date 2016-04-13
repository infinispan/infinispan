package org.infinispan.commands.remote;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ByteString;

/**
 * The {@link org.infinispan.remoting.rpc.RpcManager} only replicates commands wrapped in a {@link CacheRpcCommand}.
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
   ByteString getCacheName();

   /**
    * Set the origin of the command
    * @param origin
    */
   void setOrigin(Address origin);

   /**
    * Get the origin of the command
    * @return
    */
   Address getOrigin();

}
