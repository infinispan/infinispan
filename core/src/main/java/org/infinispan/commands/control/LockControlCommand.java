package org.infinispan.commands.control;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.concurrent.locks.TransactionalRemoteLockCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;


/**
 * LockControlCommand is a command that enables distributed locking across infinispan nodes.
  * For more details refer to: https://jira.jboss.org/jira/browse/ISPN-70 https://jira.jboss.org/jira/browse/ISPN-48
 *
 * @author Vladimir Blagojevic (<a href="mailto:vblagoje@redhat.com">vblagoje@redhat.com</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@ProtoTypeId(ProtoStreamTypeIds.LOCK_CONTROL_COMMAND)
public class LockControlCommand extends AbstractTransactionBoundaryCommand implements FlagAffectedCommand, TopologyAffectedCommand, TransactionalRemoteLockCommand {

   private static final Log log = LogFactory.getLog(LockControlCommand.class);

   public static final int COMMAND_ID = 3;

   private Collection<Object> keys;
   private boolean unlock = false;
   private long flags;

   @SuppressWarnings("unchecked")
   public LockControlCommand(Collection<?> keys, ByteString cacheName, long flags, GlobalTransaction globalTx) {
      super(cacheName, globalTx);
      this.keys = (Collection<Object>) keys;
      this.flags = flags;
      this.globalTx = globalTx;
   }

   public LockControlCommand(Object key, ByteString cacheName, long flags, GlobalTransaction globalTx) {
      this(Collections.singletonList(key), cacheName, flags, globalTx);
   }

   @ProtoFactory
   LockControlCommand(int topologyId, ByteString cacheName, GlobalTransaction globalTransaction, MarshallableCollection<Object> wrappedKeys,
                      boolean unlock, long flagsBitSetWithoutRemote) {
      super(topologyId, cacheName, globalTransaction);
      this.keys = MarshallableCollection.unwrap(wrappedKeys);
      this.unlock = unlock;
      this.flags = flagsBitSetWithoutRemote;
   }

   @ProtoField(number = 4, name = "keys")
   MarshallableCollection<Object> getWrappedKeys() {
      return MarshallableCollection.create(keys);
   }

   @ProtoField(5)
   public boolean isUnlock() {
      return unlock;
   }

   @Override
   public long getFlagsBitSet() {
      return flags;
   }

   @ProtoField(6)
   long getFlagsBitSetWithoutRemote() {
      return FlagBitSets.copyWithoutRemotableFlags(flags);
   }

   public void setGlobalTransaction(GlobalTransaction gtx) {
      globalTx = gtx;
   }

   public Collection<Object> getKeys() {
      return keys;
   }

   public boolean multipleKeys() {
      return keys.size() > 1;
   }

   public Object getSingleKey() {
      if (keys.isEmpty())
         return null;

      return keys.iterator().next();
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitLockControlCommand((TxInvocationContext) ctx, this);
   }

   @Override
   public CompletionStage<?> invokeAsync(ComponentRegistry registry) throws Throwable {
      globalTx.setRemote(true);
      RemoteTxInvocationContext ctx = createContext(registry);
      if (ctx == null) {
         return CompletableFutures.completedNull();
      }
      return registry.getInterceptorChain().running().invokeAsync(ctx, this);
   }

   @Override
   public RemoteTxInvocationContext createContext(ComponentRegistry componentRegistry) {
      TransactionTable txTable = componentRegistry.getTransactionTableRef().running();
      RemoteTransaction transaction = txTable.getRemoteTransaction(globalTx);

      if (transaction == null) {
         if (unlock) {
            log.tracef("Unlock for missing transaction %s.  Not doing anything.", globalTx);
            return null;
         }
         //create a remote tx without any modifications (we do not know modifications ahead of time)
         transaction = txTable.getOrCreateRemoteTransaction(globalTx, Collections.emptyList());
      }
      return componentRegistry.getInvocationContextFactory().running().createRemoteTxInvocationContext(transaction, getOrigin());
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
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
      return unlock == that.unlock &&
            flags == that.flags &&
            Objects.equals(keys, that.keys);
   }

   @Override
   public int hashCode() {
      return Objects.hash(super.hashCode(), keys, unlock, flags);
   }

   @Override
   public String toString() {
      return "LockControlCommand{cache=" + cacheName +
            ", keys=" + keys +
            ", flags=" + EnumUtil.prettyPrintBitSet(flags, Flag.class) +
            ", unlock=" + unlock +
            ", gtx=" + globalTx +
            "}";
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
