package org.infinispan.distribution;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * A holder for fetching transaction logs from a remote state provider
 *
 * @author Manik Surtani
 * @since 4.2.1
 */
public class RemoteTransactionLogDetails {
   final boolean drainNextCallWithoutLock;
   final List<WriteCommand> modifications;
   final Collection<PrepareCommand> pendingPreparesMap;

   public static final RemoteTransactionLogDetails DEFAULT = new RemoteTransactionLogDetails(true, Collections.<WriteCommand>emptyList(), Collections.<PrepareCommand>emptyList());

   public RemoteTransactionLogDetails(boolean drainNextCallWithoutLock, List<WriteCommand> modifications, Collection<PrepareCommand> pendingPreparesMap) {
      this.drainNextCallWithoutLock = drainNextCallWithoutLock;
      this.modifications = modifications;
      this.pendingPreparesMap = pendingPreparesMap;
   }

   public boolean isDrainNextCallWithoutLock() {
      return drainNextCallWithoutLock;
   }

   public List<WriteCommand> getModifications() {
      return modifications;
   }

   public Collection<PrepareCommand> getPendingPreparesMap() {
      return pendingPreparesMap;
   }

   @Override
   public String toString() {
      return "RemoteTransactionLogDetails{" +
            "drainNextCallWithoutLock=" + drainNextCallWithoutLock +
            ", modifications=" + (modifications == null ? "0" : modifications.size()) +
            ", pendingPrepares=" + (pendingPreparesMap == null ? "0" : pendingPreparesMap.size()) +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<RemoteTransactionLogDetails> {

      @Override
      public Integer getId() {
         return Ids.REMOTE_TX_LOG_DETAILS;
      }

      @Override
      public void writeObject(ObjectOutput output, RemoteTransactionLogDetails object) throws IOException {
         RemoteTransactionLogDetails d = object;
         output.writeBoolean(d.isDrainNextCallWithoutLock());
         output.writeObject(d.getModifications());
         output.writeObject(d.getPendingPreparesMap());

      }

      @Override
      @SuppressWarnings("unchecked")
      public RemoteTransactionLogDetails readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new RemoteTransactionLogDetails(
               input.readBoolean(),
               (List<WriteCommand>) input.readObject(),
               (Collection<PrepareCommand>) input.readObject()
         );
      }

      @Override
      public Set<Class<? extends RemoteTransactionLogDetails>> getTypeClasses() {
         return Util.<Class<? extends RemoteTransactionLogDetails>>asSet(RemoteTransactionLogDetails.class);
      }
   }
}
