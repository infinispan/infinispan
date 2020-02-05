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

   private final byte commandId;

   protected transient Address origin;

   AbstractCacheControlCommand(byte commandId) {
      this(commandId, null);
   }

   AbstractCacheControlCommand(byte commandId, Address origin) {
      this.commandId = commandId;
      this.origin = origin;
   }

   @Override
   public void setOrigin(Address origin) {
      this.origin = origin;
   }

   @Override
   public byte getCommandId() {
      return commandId;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }

   @Override
   public boolean canBlock() {
      return true;
   }
}
