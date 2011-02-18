package org.infinispan.commands.remote;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.config.Configuration;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;

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
   String getCacheName();

   /**
    * Sets up some more context for the invocation of this command, so that these components wouldn't need to be looked
    * up again later.
    * @param cfg configuration of the named cache associated with this command
    * @param cr component registry of the named cache associated with this command
    */
   void injectComponents(Configuration cfg, ComponentRegistry cr);

   /**
    * Retrieves the configuration associated with this command
    * @return a Configuration instance
    */
   Configuration getConfiguration();

   /**
    * Retrieves the component registry associated with this command
    * @return a component registry
    */
   ComponentRegistry getComponentRegistry();
   
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
