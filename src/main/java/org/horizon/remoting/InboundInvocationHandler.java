package org.horizon.remoting;

import org.horizon.commands.CacheRPCCommand;
import org.horizon.factories.scopes.Scope;
import org.horizon.factories.scopes.Scopes;
import org.horizon.statetransfer.StateTransferException;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * A globally scoped component, that is able to locate named caches and invoke remotely originating calls on the
 * appropriate cache.  The primary goal of this component is to act as a bridge between the globally scoped {@link org.horizon.remoting.RPCManager}
 * and named-cache scoped components.
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
   Object handle(CacheRPCCommand command) throws Throwable;

   /**
    * Applies state onto a named cache.  State to be read from the stream.  Implementations should NOT close the stream
    * after use.
    *
    * @param cacheName name of cache to apply state
    * @param i stream to read from
    * @throws StateTransferException in the event of problems
    */
   void applyState(String cacheName, InputStream i) throws StateTransferException;

   /**
    * Generates state from a named cache.  State to be written to the stream.  Implementations should NOT close the stream
    * after use.
    *
    * @param cacheName name of cache from which to generate state
    * @param o stream to write state to
    * @throws StateTransferException in the event of problems
    */
   void generateState(String cacheName, OutputStream o) throws StateTransferException;
}
