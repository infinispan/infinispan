package org.horizon.commands;

import org.horizon.interceptors.InterceptorChain;

/**
 * The RPCManager only replicates commands wrapped in an RPCCommand.  As a wrapper, an RPCCommand could contain a single
 * {@link org.horizon.commands.ReplicableCommand} or a List of them.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheRPCCommand extends ReplicableCommand {

   /**
    * @return true if this only wraps a single ReplicableCommand.  False if it wraps more than one.
    */
   boolean isSingleCommand();

   /**
    * A convenience method if there is only a single command being transported, i.e., if {@link #isSingleCommand()} is
    * true.  If {@link #isSingleCommand()} is false, this method throws a {@link IllegalStateException} so it should
    * only be used after testing {@link #isSingleCommand()}.
    *
    * @return a single ReplicableCommand.
    */
   ReplicableCommand getSingleCommand();

   /**
    * A more generic mechanism to get a hold of the commands wrapped.  Even if {@link #isSingleCommand()} is true, this
    * command returns a valid and usable List.
    *
    * @return a list of all commands.
    */
   ReplicableCommand[] getCommands();

   /**
    * @return the name of the cache that produced this command.  This will also be the name of the cache this command is
    *         intended for.
    */
   String getCacheName();

   void setCacheName(String name);

   /**
    * Sets the interceptor chain on which to invoke the command.
    *
    * @param interceptorChain chain to invoke command on
    */
   void setInterceptorChain(InterceptorChain interceptorChain);
}
