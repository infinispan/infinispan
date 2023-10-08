package org.infinispan.statetransfer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
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
   TransactionInfo(GlobalTransaction globalTransaction, int topologyId, MarshallableCollection<WriteCommand> wrappedModifications,
                   MarshallableCollection<Object> wrappedKeys) {
      this(
            globalTransaction,
            topologyId,
            MarshallableCollection.unwrap(wrappedModifications, ArrayList::new),
            (Set<Object>) MarshallableCollection.unwrap(wrappedKeys, HashSet::new)
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
   MarshallableCollection<WriteCommand> getWrappedModifications() {
      return MarshallableCollection.create(modifications);
   }

   public Set<Object> getLockedKeys() {
      return lockedKeys;
   }

   @ProtoField(3)
   MarshallableCollection<Object> getWrappedKeys() {
      return MarshallableCollection.create(lockedKeys);
   }

   @ProtoField(number = 4, defaultValue = "-1")
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
