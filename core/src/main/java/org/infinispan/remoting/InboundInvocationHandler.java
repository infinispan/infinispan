package org.infinispan.remoting;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.jgroups.blocks.Response;

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
    *
    * @param command command to invoke
    * @param response the asynchronous request reference from {@code org.infinispan.remoting.transport.Transport}.
    *                A {@code null} value means that the request does not expect a return value.
    * @param preserveOrder
    * @throws Throwable in the event of problems executing the command
    */
   void handle(CacheRpcCommand command, Address origin, Response response, boolean preserveOrder) throws Throwable;
}
