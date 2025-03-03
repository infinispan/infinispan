package org.infinispan.commands.topology;

import org.infinispan.commands.GlobalRpcCommand;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;

/**
 * Abstract class that is the basis for the Cache, Rebalance and Topology commands.
 *
 * @author Ryan Emerson
 * @since 11.0
 */
@Scope(Scopes.NONE)
public abstract class AbstractCacheControlCommand implements GlobalRpcCommand {

   protected transient Address origin;

   AbstractCacheControlCommand() {
      this(null);
   }

   AbstractCacheControlCommand(Address origin) {
      this.origin = origin;
   }

   @Override
   public void setOrigin(Address origin) {
      this.origin = origin;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
