package org.infinispan.commands.tx;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * An transaction boundary command that allows the retrieval of an attached
 * {@link org.infinispan.transaction.xa.GlobalTransaction}
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface TransactionBoundaryCommand extends VisitableCommand, CacheRpcCommand, TopologyAffectedCommand {

   GlobalTransaction getGlobalTransaction();

   void markTransactionAsRemote(boolean remote);
}
