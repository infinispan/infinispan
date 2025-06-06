package org.infinispan.commands.remote;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.factories.ComponentRegistry;
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
    * Invoke the command asynchronously.
    *
    * @since 11.0
    */
   CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable;

   /**
    * @return the name of the cache that produced this command.  This will also be the name of the cache this command is
    *         intended for.
    */
   ByteString getCacheName();

   /**
    * Set the origin of the command
    */
   void setOrigin(Address origin);

   /**
    * Get the origin of the command
    */
   Address getOrigin();
}
