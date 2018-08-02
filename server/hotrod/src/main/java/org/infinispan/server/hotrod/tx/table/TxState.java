package org.infinispan.server.hotrod.tx.table;

import static org.infinispan.commons.marshall.MarshallUtil.marshallCollection;
import static org.infinispan.commons.marshall.MarshallUtil.unmarshallCollection;
import static org.infinispan.server.hotrod.tx.table.Status.ACTIVE;
import static org.infinispan.server.hotrod.tx.table.Status.PREPARING;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.server.core.ExternalizerIds;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.TimeService;

import net.jcip.annotations.Immutable;

/**
 * A transaction state stored globally in all the cluster members.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
@Immutable
public class TxState {

   public static final AdvancedExternalizer<TxState> EXTERNALIZER = new Externalizer();

   private final GlobalTransaction globalTransaction;
   private final Status status;
   private final List<WriteCommand> modifications;
   private final boolean recoverable;
   private final long timeout; //ms
   private final long lastAccessTimeNs; //ns

   public TxState(GlobalTransaction globalTransaction, boolean recoverable, long timeout, TimeService timeService) {
      this(globalTransaction, ACTIVE, null, recoverable, timeout, timeService.time());
   }

   private TxState(GlobalTransaction globalTransaction, Status status, List<WriteCommand> modifications,
         boolean recoverable, long timeout, long accessTime) {
      this.globalTransaction = Objects.requireNonNull(globalTransaction);
      this.status = Objects.requireNonNull(status);
      this.modifications = modifications == null ? null : Collections.unmodifiableList(modifications);
      this.recoverable = recoverable;
      this.timeout = timeout;
      this.lastAccessTimeNs = accessTime;
   }


   public long getTimeout() {
      return timeout;
   }

   public TxState markPreparing(List<WriteCommand> modifications, TimeService timeService) {
      return new TxState(globalTransaction, PREPARING, modifications, recoverable, timeout,
            timeService.time());
   }

   public Address getOriginator() {
      return ((ClientAddress) globalTransaction.getAddress()).getLocalAddress();
   }

   public TxState setStatus(Status newStatus, boolean cleanupModification, TimeService timeService) {
      return new TxState(globalTransaction, newStatus, cleanupModification ? null : modifications, recoverable, timeout,
            timeService.time());
   }

   public Status getStatus() {
      return status;
   }

   public GlobalTransaction getGlobalTransaction() {
      return globalTransaction;
   }

   public List<WriteCommand> getModifications() {
      return modifications;
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

   public boolean hasTimedOut(long currentTimeNs) {
      long timeoutNs = TimeUnit.MILLISECONDS.toNanos(timeout);
      return lastAccessTimeNs + timeoutNs < currentTimeNs;
   }

   public boolean isRecoverable() {
      return recoverable;
   }


   @Override
   public String toString() {
      return "TxState{" +
             "globalTransaction=" + globalTransaction +
             ", status=" + status +
             ", modifications=" + modifications +
             ", recoverable=" + recoverable +
             ", timeout=" + timeout +
             ", lastAccessTime=" + lastAccessTimeNs +
             '}';
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
         Status.writeTo(output, object.status);
         marshallCollection(object.modifications, output);
         output.writeBoolean(object.recoverable);
         output.writeLong(object.timeout);
         output.writeLong(object.lastAccessTimeNs);
      }

      @Override
      public TxState readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new TxState((GlobalTransaction) input.readObject(),
               Status.readFrom(input),
               unmarshallCollection(input, ArrayList::new),
               input.readBoolean(), input.readLong(), input.readLong());
      }
   }
}
