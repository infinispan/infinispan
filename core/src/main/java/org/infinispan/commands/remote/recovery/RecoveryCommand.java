package org.infinispan.commands.remote.recovery;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.transaction.xa.recovery.RecoveryManager;

/**
 * Base class for recovery-related rpc-commands.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public abstract class RecoveryCommand extends BaseRpcCommand {

   protected RecoveryManager recoveryManager;

   private RecoveryCommand() {
      super(null); // For command id uniqueness test
   }

   protected RecoveryCommand(String cacheName) {
      super(cacheName);
   }

   public void init(RecoveryManager rm) {
      this.recoveryManager = rm;
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
