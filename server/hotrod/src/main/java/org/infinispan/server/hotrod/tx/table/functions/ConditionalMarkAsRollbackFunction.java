package org.infinispan.server.hotrod.tx.table.functions;

import static org.infinispan.server.hotrod.tx.table.Status.ERROR;
import static org.infinispan.server.hotrod.tx.table.Status.MARK_ROLLBACK;
import static org.infinispan.server.hotrod.tx.table.Status.NO_TRANSACTION;
import static org.infinispan.server.hotrod.tx.table.Status.OK;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.hotrod.tx.table.CacheXid;
import org.infinispan.server.hotrod.tx.table.Status;
import org.infinispan.server.hotrod.tx.table.TxState;

/**
 * It updates the {@link TxState}'s status to {@link Status#MARK_ROLLBACK} if the current status is the expected.
 * <p>
 * It returns {@link Status#ERROR} if it fails to update the status and {@link Status#NO_TRANSACTION} if the {@link
 * TxState} isn't found.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_CONDITION_MARK_ROLLBACK_FUNCTION)
public class ConditionalMarkAsRollbackFunction extends TxFunction {

   @ProtoField(1)
   final byte expectStatus;

   public ConditionalMarkAsRollbackFunction(Status expected) {
      this(expected.value);
   }

   @ProtoFactory
   ConditionalMarkAsRollbackFunction(byte expectStatus) {
      this.expectStatus = expectStatus;
   }

   @Override
   public Byte apply(EntryView.ReadWriteEntryView<CacheXid, TxState> view) {
      if (view.find().isPresent()) {
         TxState state = view.get();
         if (state.getStatus().value == expectStatus || state.getStatus() == MARK_ROLLBACK) {
            view.set(state.setStatus(MARK_ROLLBACK, true, timeService));
            return OK.value;
         } else {
            return ERROR.value;
         }
      } else {
         return NO_TRANSACTION.value;
      }
   }

   @Override
   public String toString() {
      return "ConditionalMarkAsRollbackFunction{" +
            "expectStatus=" + expectStatus +
            '}';
   }
}
