package org.infinispan.server.hotrod.tx.table.functions;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.hotrod.tx.table.CacheXid;
import org.infinispan.server.hotrod.tx.table.Status;
import org.infinispan.server.hotrod.tx.table.TxState;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * It creates a new {@link TxState}.
 * <p>
 * It returns {@link Status#ERROR} if the {@link TxState} already exists.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_FUNCTION_CREATE_STATE)
public class CreateStateFunction extends TxFunction {

   @ProtoField((1))
   final GlobalTransaction globalTransaction;

   @ProtoField(2)
   final long timeout;

   @ProtoField(3)
   final boolean recoverable;

   @ProtoFactory
   public CreateStateFunction(GlobalTransaction globalTransaction, boolean recoverable, long timeout) {
      this.globalTransaction = globalTransaction;
      this.recoverable = recoverable;
      this.timeout = timeout;
   }

   @Override
   public Byte apply(EntryView.ReadWriteEntryView<CacheXid, TxState> view) {
      if (view.find().isEmpty()) {
         // most common use case
         view.set(new TxState(globalTransaction, recoverable, timeout, timeService));
         return Status.OK.value;
      }
      // if it exists, we have a retry due to rebalance (isSameAs returns true), or we have a concurrent request and
      // this one will return an error.
      if (view.get().isSameAs(globalTransaction, recoverable, timeout)) {
         // force the write to the backups
         view.set(view.get());
         return Status.OK.value;
      }
      return Status.ERROR.value;
   }

   @Override
   public String toString() {
      return "CreateStateFunction{" +
            "globalTransaction=" + globalTransaction +
            ", timeout=" + timeout +
            ", recoverable=" + recoverable +
            '}';
   }
}
