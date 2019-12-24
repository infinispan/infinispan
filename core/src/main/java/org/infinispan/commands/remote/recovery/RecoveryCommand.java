package org.infinispan.commands.remote.recovery;

import org.infinispan.commands.InitializableCommand;
import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.ByteString;

/**
 * Base class for recovery-related rpc-commands.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 * @deprecated since 11.0, class will be removed with no direct replacement. BaseRpcCommand should be extended instead.
 */
@Deprecated
public abstract class RecoveryCommand extends BaseRpcCommand implements InitializableCommand {

   protected RecoveryManager recoveryManager;

   private RecoveryCommand() {
      super(null); // For command id uniqueness test
   }

   protected RecoveryCommand(ByteString cacheName) {
      super(cacheName);
   }

   @Override
   public void init(ComponentRegistry componentRegistry, boolean isRemote) {
      this.recoveryManager = componentRegistry.getRecoveryManager().running();
   }

   @Override
   public boolean isReturnValueExpected() {
      return true;
   }
}
