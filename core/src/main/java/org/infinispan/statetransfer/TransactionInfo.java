package org.infinispan.statetransfer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.MarshallUtil;
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

   private final WriteCommand[] modifications;

   private final Set<Object> lockedKeys;

   private final int topologyId;

   public TransactionInfo(GlobalTransaction globalTransaction, int topologyId, WriteCommand[] modifications, Set<Object> lockedKeys) {
      this.globalTransaction = globalTransaction;
      this.topologyId = topologyId;
      this.modifications = modifications;
      this.lockedKeys = lockedKeys;
   }

   public GlobalTransaction getGlobalTransaction() {
      return globalTransaction;
   }

   public WriteCommand[] getModifications() {
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
            ", modifications=" + Arrays.toString(modifications) +
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
         return Collections.<Class<? extends TransactionInfo>>singleton(TransactionInfo.class);
      }

      @Override
      public void writeObject(ObjectOutput output, TransactionInfo object) throws IOException {
         output.writeObject(object.globalTransaction);
         output.writeInt(object.topologyId);
         MarshallUtil.marshallArray(object.modifications, output);
         output.writeObject(object.lockedKeys);
      }

      @Override
      @SuppressWarnings("unchecked")
      public TransactionInfo readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         GlobalTransaction globalTransaction = (GlobalTransaction) input.readObject();
         int topologyId = input.readInt();
         WriteCommand[] modifications = MarshallUtil.unmarshallArray(input, WriteCommand[]::new);
         Set<Object> lockedKeys = (Set<Object>) input.readObject();
         return new TransactionInfo(globalTransaction, topologyId, modifications, lockedKeys);
      }
   }
}
