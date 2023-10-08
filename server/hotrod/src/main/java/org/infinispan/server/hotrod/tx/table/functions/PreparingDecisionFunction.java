package org.infinispan.server.hotrod.tx.table.functions;

import static org.infinispan.server.hotrod.tx.table.Status.NO_TRANSACTION;
import static org.infinispan.server.hotrod.tx.table.Status.OK;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.functional.EntryView;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.server.hotrod.tx.table.CacheXid;
import org.infinispan.server.hotrod.tx.table.Status;
import org.infinispan.server.hotrod.tx.table.TxState;

/**
 * It changes the {@link TxState} status to {@link Status#PREPARING} and stores the transaction modifications.
 * <p>
 * It returns {@link Status#NO_TRANSACTION} if the {@link TxState} isn't found and the {@link TxState#getStatus()} if
 * the current status isn't {@link Status#ACTIVE} or {@link Status#PREPARING}.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_FUNCTION_PREPARING_DECISION)
public class PreparingDecisionFunction extends TxFunction {

   private final List<WriteCommand> modifications;

   public PreparingDecisionFunction(List<WriteCommand> modifications) {
      this.modifications = modifications;
   }

   @ProtoFactory
   PreparingDecisionFunction(MarshallableCollection<WriteCommand> wrappedModifications) {
      this.modifications = MarshallableCollection.unwrap(wrappedModifications, ArrayList::new);
   }

   @ProtoField(value = 1, name = "modifications")
   MarshallableCollection<WriteCommand> getWrappedModifications() {
      return MarshallableCollection.create(modifications);
   }

   @Override
   public Byte apply(EntryView.ReadWriteEntryView<CacheXid, TxState> view) {
      if (view.find().isPresent()) {
         TxState state = view.get();
         switch (state.getStatus()) {
            case ACTIVE:
            case PREPARING:
               view.set(state.markPreparing(modifications, timeService));
               return OK.value;
            default:
               return state.getStatus().value;
         }
      } else {
         return NO_TRANSACTION.value;
      }
   }

   @Override
   public String toString() {
      return "PreparingDecisionFunction{" +
            "modifications=" + Util.toStr(modifications) +
            '}';
   }
}
