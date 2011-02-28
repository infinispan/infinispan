package org.infinispan.commands.remote;

import org.infinispan.transaction.xa.recovery.RecoveryManager;

/**
 * Base class for recovery-related rpc-commands.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public abstract class RecoveryCommand extends BaseRpcCommand {

   protected RecoveryManager recoveryManager;

   public RecoveryCommand() {
   }

   public void init(RecoveryManager rm) {
      this.recoveryManager = rm;
   }
}
