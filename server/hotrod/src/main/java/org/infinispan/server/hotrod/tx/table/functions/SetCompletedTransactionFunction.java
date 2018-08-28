package org.infinispan.server.hotrod.tx.table.functions;

import static org.infinispan.server.hotrod.tx.table.Status.COMMITTED;
import static org.infinispan.server.hotrod.tx.table.Status.NO_TRANSACTION;
import static org.infinispan.server.hotrod.tx.table.Status.OK;
import static org.infinispan.server.hotrod.tx.table.Status.ROLLED_BACK;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.functional.EntryView;
import org.infinispan.server.core.ExternalizerIds;
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
public class SetCompletedTransactionFunction extends TxFunction {

   public static final AdvancedExternalizer<SetCompletedTransactionFunction> EXTERNALIZER = new Externalizer();

   private final boolean committed;

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

   private static class Externalizer implements AdvancedExternalizer<SetCompletedTransactionFunction> {

      @Override
      public Set<Class<? extends SetCompletedTransactionFunction>> getTypeClasses() {
         return Collections.singleton(SetCompletedTransactionFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.COMPLETE_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, SetCompletedTransactionFunction object) throws IOException {
         output.writeBoolean(object.committed);
      }

      @Override
      public SetCompletedTransactionFunction readObject(ObjectInput input) throws IOException {
         return new SetCompletedTransactionFunction(input.readBoolean());
      }
   }

}
