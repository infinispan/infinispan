package org.infinispan.server.hotrod.tx.table.functions;

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
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * It creates a new {@link TxState}.
 * <p>
 * It returns {@link Status#ERROR} if the {@link TxState} already exists.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public class CreateStateFunction extends TxFunction {

   public static final AdvancedExternalizer<CreateStateFunction> EXTERNALIZER = new Externalizer();

   private final GlobalTransaction globalTransaction;
   private final long timeout;
   private final boolean recoverable;

   public CreateStateFunction(GlobalTransaction globalTransaction, boolean recoverable, long timeout) {
      this.globalTransaction = globalTransaction;
      this.recoverable = recoverable;
      this.timeout = timeout;
   }

   @Override
   public Byte apply(EntryView.ReadWriteEntryView<CacheXid, TxState> view) {
      if (view.find().isPresent()) {
         return Status.ERROR.value;
      }
      view.set(new TxState(globalTransaction, recoverable, timeout, timeService));
      return Status.OK.value;
   }

   private static class Externalizer implements AdvancedExternalizer<CreateStateFunction> {

      @Override
      public Set<Class<? extends CreateStateFunction>> getTypeClasses() {
         return Collections.singleton(CreateStateFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.CREATE_STATE_FUNCTION;
      }

      @Override
      public void writeObject(ObjectOutput output, CreateStateFunction object) throws IOException {
         output.writeObject(object.globalTransaction);
         output.writeBoolean(object.recoverable);
         output.writeLong(object.timeout);
      }

      @Override
      public CreateStateFunction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new CreateStateFunction((GlobalTransaction) input.readObject(), input.readBoolean(), input.readLong());
      }
   }
}
