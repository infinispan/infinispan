package org.infinispan.server.hotrod.tx.table.functions;

import static org.infinispan.server.hotrod.tx.table.Status.NO_TRANSACTION;
import static org.infinispan.server.hotrod.tx.table.Status.OK;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.functional.EntryView;
import org.infinispan.server.core.ExternalizerIds;
import org.infinispan.server.hotrod.tx.table.CacheXid;
import org.infinispan.server.hotrod.tx.table.TxState;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public class PreparingDecisionFunction extends TxFunction {

   public static final AdvancedExternalizer<PreparingDecisionFunction> EXTERNALIZER = new Externalizer();

   private final List<WriteCommand> modifications;

   public PreparingDecisionFunction(List<WriteCommand> modifications) {
      this.modifications = modifications;
   }

   @Override
   public Byte apply(EntryView.ReadWriteEntryView<CacheXid, TxState> view) {
      if (view.find().isPresent()) {
         TxState state = view.get();
         switch (state.getStatus()) {
            case ACTIVE:
               view.set(state.markPreparing(modifications, timeService));
            case PREPARING:
               return OK.value;
            default:
               return state.getStatus().value;
         }
      } else {
         return NO_TRANSACTION.value;
      }
   }

   private static class Externalizer implements AdvancedExternalizer<PreparingDecisionFunction> {

      @Override
      public Set<Class<? extends PreparingDecisionFunction>> getTypeClasses() {
         return Collections.singleton(PreparingDecisionFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.PREPARING_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, PreparingDecisionFunction object) throws IOException {
         MarshallUtil.marshallCollection(object.modifications, output);
      }

      @Override
      public PreparingDecisionFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new PreparingDecisionFunction(MarshallUtil.unmarshallCollection(input, ArrayList::new));
      }
   }


}
