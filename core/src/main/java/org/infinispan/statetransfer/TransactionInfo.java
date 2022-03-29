package org.infinispan.statetransfer;

import static org.infinispan.commons.marshall.MarshallUtil.marshallCollection;
import static org.infinispan.commons.marshall.MarshallUtil.unmarshallCollection;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * A representation of a transaction that is suitable for transferring between a StateProvider and a StateConsumer
 * running on different members of the same cache.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class TransactionInfo {

   private final GlobalTransaction globalTransaction;

   private final List<WriteCommand> modifications;

   private final Set<Object> lockedKeys;

   private final int topologyId;

   public TransactionInfo(GlobalTransaction globalTransaction, int topologyId, List<WriteCommand> modifications, Set<Object> lockedKeys) {
      this.globalTransaction = globalTransaction;
      this.topologyId = topologyId;
      this.modifications = modifications;
      this.lockedKeys = lockedKeys;
   }

   public GlobalTransaction getGlobalTransaction() {
      return globalTransaction;
   }

   public List<WriteCommand> getModifications() {
      return modifications;
   }

   public Set<Object> getLockedKeys() {
      return lockedKeys;
   }

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

   public static class Externalizer extends AbstractExternalizer<TransactionInfo> {

      @Override
      public Integer getId() {
         return Ids.TRANSACTION_INFO;
      }

      @Override
      public Set<Class<? extends TransactionInfo>> getTypeClasses() {
         return Collections.singleton(TransactionInfo.class);
      }

      @Override
      public void writeObject(ObjectOutput output, TransactionInfo object) throws IOException {
         output.writeObject(object.globalTransaction);
         output.writeInt(object.topologyId);
         marshallCollection(object.modifications, output);
         marshallCollection(object.lockedKeys, output);
      }

      @Override
      public TransactionInfo readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         GlobalTransaction globalTransaction = (GlobalTransaction) input.readObject();
         int topologyId = input.readInt();
         List<WriteCommand> modifications = unmarshallCollection(input, ArrayList::new);
         Set<Object> lockedKeys = unmarshallCollection(input, HashSet::new);
         return new TransactionInfo(globalTransaction, topologyId, modifications, lockedKeys);
      }
   }
}
