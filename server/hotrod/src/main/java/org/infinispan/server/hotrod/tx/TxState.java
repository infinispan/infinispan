package org.infinispan.server.hotrod.tx;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.transaction.Status;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.tx.Util;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.core.ExternalizerIds;
import org.infinispan.transaction.xa.GlobalTransaction;

import net.jcip.annotations.Immutable;

/**
 * A transaction state stored globally in all the cluster members.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
@Immutable
public class TxState {

   public static final AdvancedExternalizer<TxState> EXTERNALIZER = new Externalizer();
   private final GlobalTransaction globalTransaction;
   private final int status;
   private final List<WriteCommand> modifications;

   TxState(GlobalTransaction globalTransaction) {
      this(globalTransaction, Status.STATUS_ACTIVE, null);
   }

   private TxState(GlobalTransaction globalTransaction, int status, List<WriteCommand> modifications) {
      this.globalTransaction = globalTransaction;
      this.status = status;
      this.modifications = modifications;
   }


   private static List<WriteCommand> copyModifications(Collection<WriteCommand> modifications) {
      return modifications == null || modifications.isEmpty() ?
            null :
            Collections.unmodifiableList(new ArrayList<>(modifications));
   }

   public int status() {
      return status;
   }

   public Address getOriginator() {
      return globalTransaction.getAddress();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || getClass() != o.getClass()) {
         return false;
      }

      TxState txState = (TxState) o;

      return Objects.equals(status, txState.status) &&
            Objects.equals(globalTransaction, txState.globalTransaction);
   }

   @Override
   public int hashCode() {
      int result = Objects.hashCode(globalTransaction);
      result = 31 * result + Objects.hashCode(status);
      return result;
   }

   @Override
   public String toString() {
      return "TxState{" +
            "globalTransaction=" + globalTransaction +
            ", status=" + Util.transactionStatusToString(status) +
            ", modifications=" + modifications +
            '}';
   }

   GlobalTransaction getGlobalTransaction() {
      return globalTransaction;
   }

   List<WriteCommand> getModifications() {
      return modifications;
   }

   TxState prepare(Collection<WriteCommand> modifications) {
      return status == Status.STATUS_ACTIVE ?
            new TxState(globalTransaction, Status.STATUS_PREPARED, copyModifications(modifications)) :
            null;
   }

   TxState commit() {
      return status == Status.STATUS_PREPARED ?
            new TxState(globalTransaction, Status.STATUS_COMMITTED, null) :
            null;
   }

   TxState rollback() {
      return status == Status.STATUS_ACTIVE || status == Status.STATUS_PREPARED ?
            new TxState(globalTransaction, Status.STATUS_ROLLEDBACK, null) :
            null;
   }

   private static class Externalizer implements AdvancedExternalizer<TxState> {

      @Override
      public Set<Class<? extends TxState>> getTypeClasses() {
         return Collections.singleton(TxState.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.TX_STATE;
      }

      @Override
      public void writeObject(ObjectOutput output, TxState object) throws IOException {
         output.writeObject(object.globalTransaction);
         UnsignedNumeric.writeUnsignedInt(output, object.status);
         MarshallUtil.marshallCollection(object.modifications, output);
      }

      @Override
      public TxState readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new TxState((GlobalTransaction) input.readObject(),
               UnsignedNumeric.readUnsignedInt(input),
               MarshallUtil.unmarshallCollection(input, ArrayList::new));
      }
   }
}
