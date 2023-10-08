package org.infinispan.statetransfer;

import java.util.List;
import java.util.Set;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableList;
import org.infinispan.marshall.protostream.impl.MarshallableSet;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * A representation of a transaction that is suitable for transferring between a StateProvider and a StateConsumer
 * running on different members of the same cache.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@ProtoTypeId(ProtoStreamTypeIds.TRANSACTION_INFO)
public class TransactionInfo {

   private final GlobalTransaction globalTransaction;

   private final List<WriteCommand> modifications;

   private final Set<Object> lockedKeys;

   private final int topologyId;

   @ProtoFactory
   TransactionInfo(GlobalTransaction globalTransaction, int topologyId, MarshallableList<WriteCommand> wrappedModifications,
                   MarshallableSet<Object> wrappedKeys) {
      this(
            globalTransaction,
            topologyId,
            MarshallableList.unwrap(wrappedModifications),
            MarshallableSet.unwrap(wrappedKeys)
      );
   }

   public TransactionInfo(GlobalTransaction globalTransaction, int topologyId, List<WriteCommand> modifications, Set<Object> lockedKeys) {
      this.globalTransaction = globalTransaction;
      this.topologyId = topologyId;
      this.modifications = modifications;
      this.lockedKeys = lockedKeys;
   }

   @ProtoField(1)
   public GlobalTransaction getGlobalTransaction() {
      return globalTransaction;
   }

   public List<WriteCommand> getModifications() {
      return modifications;
   }

   @ProtoField(2)
   MarshallableList<WriteCommand> getWrappedModifications() {
      return MarshallableList.create(modifications);
   }

   public Set<Object> getLockedKeys() {
      return lockedKeys;
   }

   @ProtoField(3)
   MarshallableSet<Object> getWrappedKeys() {
      return MarshallableSet.create(lockedKeys);
   }

   @ProtoField(4)
   public int getTopologyId() {
      return topologyId;
   }

   @Override
   public String toString() {
      return "TransactionInfo{" +
            "globalTransaction=" + globalTransaction +
            ", topologyId=" + topologyId +
            ", modifications=" + modifications +
            ", lockedKeys=" + lockedKeys +
            '}';
   }
}
