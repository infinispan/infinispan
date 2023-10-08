package org.infinispan.server.hotrod.tx.table;

import static org.infinispan.server.hotrod.tx.table.Status.ACTIVE;
import static org.infinispan.server.hotrod.tx.table.Status.PREPARING;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.time.TimeService;
import org.infinispan.marshall.protostream.impl.MarshallableList;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

import net.jcip.annotations.Immutable;

/**
 * A transaction state stored globally in all the cluster members.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
@Immutable
@ProtoTypeId(ProtoStreamTypeIds.SERVER_HR_TX_STATE)
public class TxState {

   @ProtoField(1)
   final GlobalTransaction globalTransaction;

   @ProtoField(2)
   final Status status;

   @ProtoField(3)
   final boolean recoverable;

   @ProtoField(4)
   final long timeout; //ms

   @ProtoField(5)
   final long lastAccessTimeNs; //ns

   private final List<WriteCommand> modifications;

   public TxState(GlobalTransaction globalTransaction, boolean recoverable, long timeout, TimeService timeService) {
      this(globalTransaction, ACTIVE, null, recoverable, timeout, timeService.time());
   }

   private TxState(GlobalTransaction globalTransaction, Status status, List<WriteCommand> modifications,
         boolean recoverable, long timeout, long accessTime) {
      this.globalTransaction = Objects.requireNonNull(globalTransaction);
      this.status = Objects.requireNonNull(status);
      this.modifications = modifications == null ? null : List.copyOf(modifications);
      this.recoverable = recoverable;
      this.timeout = timeout;
      lastAccessTimeNs = accessTime;
   }

   @ProtoFactory
   TxState(GlobalTransaction globalTransaction, Status status, boolean recoverable, long timeout, long lastAccessTimeNs,
           MarshallableList<WriteCommand> wrappedModifications) {
      this(globalTransaction, status, MarshallableList.unwrap(wrappedModifications), recoverable, timeout, lastAccessTimeNs);
   }

   @ProtoField(number = 6, name = "modification")
   MarshallableList<WriteCommand> getWrappedModifications() {
      return MarshallableList.create(modifications);
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

   public boolean isSameAs(GlobalTransaction globalTransaction, boolean recoverable, long timeout) {
      return this.timeout == timeout &&
            this.recoverable == recoverable &&
            this.globalTransaction.equals(globalTransaction) &&
            status == ACTIVE;
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
}
