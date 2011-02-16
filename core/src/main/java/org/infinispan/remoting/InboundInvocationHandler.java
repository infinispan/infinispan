package org.infinispan.remoting;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.responses.Response;
import org.infinispan.statetransfer.StateTransferException;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * A globally scoped component, that is able to locate named caches and invoke remotely originating calls on the
 * appropriate cache.  The primary goal of this component is to act as a bridge between the globally scoped {@link
 * org.infinispan.remoting.rpc.RpcManager} and named-cache scoped components.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public interface InboundInvocationHandler {

   /**
    * Invokes a command on the cache, from a remote source.
    *
    * @param command command to invoke
    * @return results, if any, from the invocation
    * @throws Throwable in the event of problems executing the command
    */
   Response handle(CacheRpcCommand command) throws Throwable;

   /**
    * Applies state onto a named cache.  State to be read from the stream.  Implementations should NOT close the stream
    * after use.
    *
    * @param cacheName name of cache to apply state
    * @param i         stream to read from
    * @throws StateTransferException in the event of problems
    */
   void applyState(String cacheName, InputStream i) throws StateTransferException;

   /**
    * Generates state from a named cache.  State to be written to the stream.  Implementations should NOT close the
    * stream after use.
    *
    * @param cacheName name of cache from which to generate state
    * @param o         stream to write state to
    * @throws StateTransferException in the event of problems
    */
   void generateState(String cacheName, OutputStream o) throws StateTransferException;

   /**
    * Calling this method should block if the invocation handler implementation has been queueing commands for a given
    * named cache and is in the process of flushing this queue.  It would block until the queue has been drained.
    * @param cacheName name of the cache for which the handler would be queueing requests.
    */
   void blockTillNoLongerRetrying(String cacheName);
}
