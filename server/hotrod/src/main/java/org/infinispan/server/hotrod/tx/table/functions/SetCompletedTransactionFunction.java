package org.infinispan.server.hotrod.tx.table.functions;

import static org.infinispan.server.hotrod.tx.table.Status.COMMITTED;
import static org.infinispan.server.hotrod.tx.table.Status.NO_TRANSACTION;
import static org.infinispan.server.hotrod.tx.table.Status.OK;
import static org.infinispan.server.hotrod.tx.table.Status.ROLLED_BACK;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.hotrod.tx.table.CacheXid;
import org.infinispan.server.hotrod.tx.table.Status;
import org.infinispan.server.hotrod.tx.table.TxState;

/**
 * It marks the transaction as completed in {@link TxState} by setting its status to {@link Status#COMMITTED} or {@link
 * Status#ROLLED_BACK}.
 * <p>
 * It doesn't check the {@link TxState} current status since it should be only invoked when the transaction completes.
 * And it returns {@link Status#NO_TRANSACTION} if the {@link TxState} doesn't exist.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_FUNCTION_SET_COMPLETED_TX)
public class SetCompletedTransactionFunction extends TxFunction {

   @ProtoField(value = 1, defaultValue = "false")
   final boolean committed;

   @ProtoFactory
   public SetCompletedTransactionFunction(boolean committed) {
      this.committed = committed;
   }

   @Override
   public Byte apply(EntryView.ReadWriteEntryView<CacheXid, TxState> view) {
      if (view.find().isPresent()) {
         TxState state = view.get();
         view.set(state.setStatus(committed ? COMMITTED : ROLLED_BACK, true, timeService));
         return OK.value;
      } else {
         return NO_TRANSACTION.value;
      }
   }

   @Override
   public String toString() {
      return "SetCompletedTransactionFunction{" +
            "committed=" + committed +
            '}';
   }
}
