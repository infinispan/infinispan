package org.infinispan.server.hotrod.tx.table.functions;

import static org.infinispan.server.hotrod.tx.table.Status.ERROR;
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
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public class ConditionalMarkAsRollbackFunction extends TxFunction {

   public static final AdvancedExternalizer<ConditionalMarkAsRollbackFunction> EXTERNALIZER = new Externalizer();

   private final byte expectStatus;

   public ConditionalMarkAsRollbackFunction(Status expected) {
      this(expected.value);
   }

   private ConditionalMarkAsRollbackFunction(byte expectStatus) {
      this.expectStatus = expectStatus;
   }


   @Override
   public Byte apply(EntryView.ReadWriteEntryView<CacheXid, TxState> view) {
      if (view.find().isPresent()) {
         TxState state = view.get();
         if (state.getStatus().value == expectStatus) {
            view.set(state.setStatus(MARK_ROLLBACK, true, timeService));
            return OK.value;
         } else {
            return ERROR.value;
         }
      } else {
         return NO_TRANSACTION.value;
      }
   }

   private static class Externalizer implements AdvancedExternalizer<ConditionalMarkAsRollbackFunction> {

      @Override
      public Set<Class<? extends ConditionalMarkAsRollbackFunction>> getTypeClasses() {
         return Collections.singleton(ConditionalMarkAsRollbackFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CONDITIONAL_MARK_ROLLBACK_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, ConditionalMarkAsRollbackFunction object) throws IOException {
         output.writeByte(object.expectStatus);
      }

      @Override
      public ConditionalMarkAsRollbackFunction readObject(ObjectInput input) throws IOException {
         return new ConditionalMarkAsRollbackFunction(input.readByte());
      }
   }

}
