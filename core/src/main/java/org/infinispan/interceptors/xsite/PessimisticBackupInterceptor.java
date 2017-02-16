package org.infinispan.interceptors.xsite;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.context.impl.TxInvocationContext;

/**
 * Handles x-site data backups for pessimistic transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class PessimisticBackupInterceptor extends BaseBackupInterceptor {

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      //for pessimistic transaction we don't do a 2PC (as we already own the remote lock) but just
      //a 1PC
      throw new IllegalStateException("This should never happen!");
   }
}
