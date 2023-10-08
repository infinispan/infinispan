package org.infinispan.commands.tx;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * An abstract transaction boundary command that holds a reference to a {@link org.infinispan.transaction.xa.GlobalTransaction}
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public abstract class AbstractTransactionBoundaryCommand implements TransactionBoundaryCommand {

   private static final Log log = LogFactory.getLog(AbstractTransactionBoundaryCommand.class);

   protected final ByteString cacheName;
   protected GlobalTransaction globalTx;
   private Address origin;
   private int topologyId = -1;

   protected AbstractTransactionBoundaryCommand(ByteString cacheName) {
      this(-1, cacheName, null);
   }

   protected AbstractTransactionBoundaryCommand(ByteString cacheName, GlobalTransaction globalTx) {
      this(-1, cacheName, globalTx);
   }

   protected AbstractTransactionBoundaryCommand(int topologyId, ByteString cacheName, GlobalTransaction globalTx) {
      this.topologyId = topologyId;
      this.cacheName = cacheName;
      this.globalTx = globalTx;
   }

   @Override
   @ProtoField(1)
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public void setTopologyId(int topologyId) {
      this.topologyId = topologyId;
   }

   @Override
   @ProtoField(2)
   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   @ProtoField(3)
   public GlobalTransaction getGlobalTransaction() {
      return globalTx;
   }

   @Override
   public void markTransactionAsRemote(boolean isRemote) {
      globalTx.setRemote(isRemote);
   }

   /**
    * This is what is returned to remote callers when an invalid RemoteTransaction is encountered.  Can happen if a
    * remote node propagates a transactional call to the current node, and the current node has no idea of the transaction
    * in question.  Can happen during rehashing, when ownerships are reassigned during a transactions.
    *
    * Returning a null usually means the transactional command succeeded.
    * @return return value to respond to a remote caller with if the transaction context is invalid.
    */
   protected Object invalidRemoteTxReturnValue(TransactionTable txTable) {
      return null;
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      globalTx.setRemote(true);
      TransactionTable txTable = registry.getTransactionTableRef().running();
      RemoteTransaction transaction = txTable.getRemoteTransaction(globalTx);
      if (transaction == null) {
         if (log.isTraceEnabled()) log.tracef("Did not find a RemoteTransaction for %s", globalTx);
         return CompletableFuture.completedFuture(invalidRemoteTxReturnValue(txTable));
      }
      visitRemoteTransaction(transaction);
      InvocationContextFactory icf = registry.getInvocationContextFactory().running();
      RemoteTxInvocationContext ctx = icf.createRemoteTxInvocationContext(transaction, getOrigin());

      if (log.isTraceEnabled()) log.tracef("About to execute tx command %s", this);
      return registry.getInterceptorChain().running().invokeAsync(ctx, this);
   }

   protected void visitRemoteTransaction(RemoteTransaction tx) {
      // to be overridden
   }

   @Override
   public LoadType loadType() {
      throw new UnsupportedOperationException();
   }

   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AbstractTransactionBoundaryCommand that = (AbstractTransactionBoundaryCommand) o;
      return this.globalTx.equals(that.globalTx);
   }

   public int hashCode() {
      return globalTx.hashCode();
   }

   @Override
   public String toString() {
      return "gtx=" + globalTx +
            ", cacheName='" + cacheName + '\'' +
            ", topologyId=" + topologyId +
            '}';
   }

   @Override
   public Address getOrigin() {
      return origin;
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
