package org.infinispan.server.hotrod.tx.table.functions;

import static org.infinispan.server.hotrod.tx.table.Status.NO_TRANSACTION;
import static org.infinispan.server.hotrod.tx.table.Status.OK;
import static org.infinispan.server.hotrod.tx.table.Status.PREPARED;
import static org.infinispan.server.hotrod.tx.table.Status.PREPARING;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.hotrod.tx.table.CacheXid;
import org.infinispan.server.hotrod.tx.table.Status;
import org.infinispan.server.hotrod.tx.table.TxState;

import jakarta.transaction.TransactionManager;

/**
 * It sets the transaction as successful prepared.
 * <p>
 * The {@link TxState} status must be {@link Status#PREPARING}. If not, it returns the current status.
 * <p>
 * If the {@link TxState} doesn't exist, it returns {@link Status#NO_TRANSACTION}.
 * <p>
 * Note that the {@link Status#PREPARED} doesn't mean the transaction can commit or not. The decision is made by the
 * client {@link TransactionManager}.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_FUNCTION_PREPARED)
public class SetPreparedFunction extends TxFunction {

   @Override
   public Byte apply(EntryView.ReadWriteEntryView<CacheXid, TxState> view) {
      if (view.find().isPresent()) {
         TxState state = view.get();
         if (state.getStatus() != PREPARING) {
            return state.getStatus().value;
         }
         view.set(state.setStatus(PREPARED, false, timeService));
         return OK.value;
      } else {
         return NO_TRANSACTION.value;
      }
   }

   @Override
   public String toString() {
      return "SetPreparedFunction{}";
   }
}
