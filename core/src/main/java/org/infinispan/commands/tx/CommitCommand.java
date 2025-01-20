package org.infinispan.commands.tx;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commands.Visitor;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;

/**
 * Command corresponding to the 2nd phase of 2PC.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.COMMIT_COMMAND)
public class CommitCommand extends AbstractTransactionBoundaryCommand {
   public static final byte COMMAND_ID = 14;

   //IRAC versions are segment based and they are generated during prepare phase. We can save some space here.
   private Map<Integer, IracMetadata> iracMetadataMap;

   public CommitCommand(ByteString cacheName, GlobalTransaction globalTransaction) {
      super(cacheName, globalTransaction);
   }

   @ProtoFactory
   CommitCommand(int topologyId, ByteString cacheName, GlobalTransaction globalTransaction, MarshallableMap<Integer, IracMetadata> iracMetadataMap) {
      super(topologyId, cacheName, globalTransaction);
      this.iracMetadataMap = MarshallableMap.unwrap(iracMetadataMap);
   }

   @ProtoField(4)
   MarshallableMap<Integer, IracMetadata> getIracMetadataMap() {
      return MarshallableMap.create(iracMetadataMap);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitCommitCommand((TxInvocationContext) ctx, this);
   }

   @Override
   protected Object invalidRemoteTxReturnValue(TransactionTable txTable) {
      TransactionTable.CompletedTransactionStatus txStatus = txTable.getCompletedTransactionStatus(globalTx);
      switch (txStatus) {
         case COMMITTED:
            // The transaction was already committed on this node
            return null;
         case ABORTED:
            throw CONTAINER.remoteTransactionAlreadyRolledBack(globalTx);
         case EXPIRED:
            throw CONTAINER.remoteTransactionStatusMissing(globalTx);
         default:  // NOT_COMPLETED
            throw new IllegalStateException("Remote transaction not found: " + globalTx);
      }
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public String toString() {
      return "CommitCommand{" +
            "iracMetadataMap=" + iracMetadataMap + ", " +
            super.toString();
   }

   public void addIracMetadata(int segment, IracMetadata metadata) {
      if (iracMetadataMap == null) {
         iracMetadataMap = new HashMap<>();
      }
      iracMetadataMap.put(segment, metadata);
   }

   public IracMetadata getIracMetadata(int segment) {
      return iracMetadataMap != null ? iracMetadataMap.get(segment) : null;
   }
}
