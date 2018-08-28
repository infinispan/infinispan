package org.infinispan.server.hotrod.tx.table.functions;

import static org.infinispan.server.hotrod.tx.table.Status.MARK_COMMIT;
import static org.infinispan.server.hotrod.tx.table.Status.MARK_ROLLBACK;
import static org.infinispan.server.hotrod.tx.table.Status.NO_TRANSACTION;
import static org.infinispan.server.hotrod.tx.table.Status.OK;

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
 * It sets the transaction decision in {@link TxState}.
 * <p>
 * The decision can be {@link Status#MARK_ROLLBACK} or {@link Status#MARK_COMMIT} and the {@link TxState} status must be
 * valid. If not, it returns the current {@link TxState} status.
 * <p>
 * If the {@link TxState} doesn't exists, it returns {@link Status#NO_TRANSACTION}.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public class SetDecisionFunction extends TxFunction {

   public static final AdvancedExternalizer<SetDecisionFunction> EXTERNALIZER = new Externalizer();

   private final boolean commit;

   public SetDecisionFunction(boolean commit) {
      this.commit = commit;
   }


   @Override
   public Byte apply(EntryView.ReadWriteEntryView<CacheXid, TxState> view) {
      if (view.find().isPresent()) {
         return commit ?
               advanceToCommit(view) :
               advanceToRollback(view);
      } else {
         return NO_TRANSACTION.value;
      }
   }

   private byte advanceToRollback(EntryView.ReadWriteEntryView<CacheXid, TxState> view) {
      TxState state = view.get();
      Status prevState = state.getStatus();
      //we can rollback if the transaction is running or prepared
      switch (prevState) {
         case ACTIVE:
         case PREPARED:
         case PREPARING:
            view.set(state.setStatus(MARK_ROLLBACK, true, timeService));
         case MARK_ROLLBACK:
            return OK.value;
         default:
            //any other status, we return it to the caller to decide what to do.
            return prevState.value;
      }
   }

   private byte advanceToCommit(EntryView.ReadWriteEntryView<CacheXid, TxState> view) {
      TxState state = view.get();
      Status prevState = state.getStatus();
      //we can advance to commit decision if it is prepared or the commit decision is already set
      switch (prevState) {
         case PREPARED:  //two-phase-commit
         case PREPARING: //one-phase-commit
            view.set(state.setStatus(MARK_COMMIT, false, timeService));
         case MARK_COMMIT:
            return OK.value;
         default:
            //any other status, we return it to the caller to decide what to do.
            return prevState.value;
      }
   }

   private static class Externalizer implements AdvancedExternalizer<SetDecisionFunction> {

      @Override
      public Set<Class<? extends SetDecisionFunction>> getTypeClasses() {
         return Collections.singleton(SetDecisionFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.DECISION_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, SetDecisionFunction object) throws IOException {
         output.writeBoolean(object.commit);
      }

      @Override
      public SetDecisionFunction readObject(ObjectInput input) throws IOException {
         return new SetDecisionFunction(input.readBoolean());
      }
   }

}
