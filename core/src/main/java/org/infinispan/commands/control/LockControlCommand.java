package org.infinispan.commands.control;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.marshall.MarshalledEntryUtil;
import org.infinispan.marshall.core.MarshalledEntryFactory;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.locks.TransactionalRemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;


/**
 * LockControlCommand is a command that enables distributed locking across infinispan nodes.
 * <p/>
 * For more details refer to: https://jira.jboss.org/jira/browse/ISPN-70 https://jira.jboss.org/jira/browse/ISPN-48
 *
 * @author Vladimir Blagojevic (<a href="mailto:vblagoje@redhat.com">vblagoje@redhat.com</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class LockControlCommand extends AbstractTransactionBoundaryCommand implements FlagAffectedCommand, TopologyAffectedCommand, TransactionalRemoteLockCommand {

   private static final Log log = LogFactory.getLog(LockControlCommand.class);

   public static final int COMMAND_ID = 3;

   private List<Object> keys;
   private boolean unlock = false;
   private long flags = EnumUtil.EMPTY_BIT_SET;

   private LockControlCommand() {
      super(null); // For command id uniqueness test
   }

   public LockControlCommand(ByteString cacheName) {
      super(cacheName);
   }

   public LockControlCommand(Collection<?> keys, ByteString cacheName, long flags, GlobalTransaction gtx) {
      super(cacheName);
      if (keys != null) {
         //building defensive copies is here in order to support replaceKey operation
         this.keys = new ArrayList<>(keys);
      } else {
         this.keys = Collections.emptyList();
      }
      this.flags = flags;
      this.globalTx = gtx;
   }

   public LockControlCommand(Object key, ByteString cacheName, long flags, GlobalTransaction gtx) {
      this(cacheName);
      this.keys = new ArrayList<>(1);
      this.keys.add(key);
      this.flags = flags;
      this.globalTx = gtx;
   }

   public void setGlobalTransaction(GlobalTransaction gtx) {
      globalTx = gtx;
   }

   public Collection<Object> getKeys() {
      return keys;
   }

   public void replaceKey(Object oldKey, Object replacement) {
      int i = keys.indexOf(oldKey);
      if (i >= 0) {
         keys.set(i, replacement);
      }
   }

   public void replaceKeys(Map<Object, Object> replacements) {
      for (int i = 0; i < keys.size(); i++) {
         Object replacement = replacements.get(keys.get(i));
         if (replacement != null) {
            keys.set(i, replacement);
         }
      }
   }

   public boolean multipleKeys() {
      return keys.size() > 1;
   }

   public Object getSingleKey() {
      if (keys.size() == 0)
         return null;

      return keys.get(0);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitLockControlCommand((TxInvocationContext) ctx, this);
   }

   @Override
   public CompletableFuture<Object> invokeAsync() throws Throwable {
      RemoteTxInvocationContext ctx = createContext();
      if (ctx == null) {
         return CompletableFutures.completedNull();
      }
      return invoker.invokeAsync(ctx, this);
   }

   @Override
   public RemoteTxInvocationContext createContext() {
      RemoteTransaction transaction = txTable.getRemoteTransaction(globalTx);

      if (transaction == null) {
         if (unlock) {
            log.tracef("Unlock for missing transaction %s.  Not doing anything.", globalTx);
            return null;
         }
         //create a remote tx without any modifications (we do not know modifications ahead of time)
         transaction = txTable.getOrCreateRemoteTransaction(globalTx, null);
      }
      return icf.createRemoteTxInvocationContext(transaction, getOrigin());
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public void writeTo(UserObjectOutput output, MarshalledEntryFactory entryFactory) throws IOException {
      super.writeTo(output, entryFactory);
      output.writeBoolean(unlock);
      output.writeUserCollection(keys, UserObjectOutput::writeKey);
      output.writeLong(FlagBitSets.copyWithoutRemotableFlags(flags));
   }

   @Override
   @SuppressWarnings("unchecked")
   public void readFrom(ObjectInput input) throws IOException, ClassNotFoundException {
      super.readFrom(input);
      unlock = input.readBoolean();
      keys = MarshallUtil.unmarshallCollection(input, ArrayList::new, MarshalledEntryUtil::readKey);
      flags = input.readLong();
   }

   public boolean isUnlock() {
      return unlock;
   }

   public void setUnlock(boolean unlock) {
      this.unlock = unlock;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      if (!super.equals(o)) return false;

      LockControlCommand that = (LockControlCommand) o;

      if (unlock != that.unlock) return false;
      if (flags != that.flags) return false;
      if (!keys.equals(that.keys)) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + keys.hashCode();
      result = 31 * result + (unlock ? 1 : 0);
      result = 31 * result + (int) (flags ^ (flags >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return new StringBuilder()
         .append("LockControlCommand{cache=").append(cacheName)
         .append(", keys=").append(keys)
         .append(", flags=").append(EnumUtil.prettyPrintBitSet(flags, Flag.class))
         .append(", unlock=").append(unlock)
         .append(", gtx=").append(globalTx)
         .append("}")
         .toString();
   }

   @Override
   public long getFlagsBitSet() {
      return flags;
   }

   @Override
   public void setFlagsBitSet(long bitSet) {
      this.flags = bitSet;
   }

   @Override
   public Collection<?> getKeysToLock() {
      return unlock ? Collections.emptyList() : Collections.unmodifiableCollection(keys);
   }

   @Override
   public Object getKeyLockOwner() {
      return globalTx;
   }

   @Override
   public boolean hasZeroLockAcquisition() {
      return hasAnyFlag(FlagBitSets.ZERO_LOCK_ACQUISITION_TIMEOUT);
   }

   @Override
   public boolean hasSkipLocking() {
      return hasAnyFlag(FlagBitSets.SKIP_LOCKING); //is it possible??
   }
}
