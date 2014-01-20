package org.infinispan.commands.tx;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.TimSort;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Command corresponding to the 1st phase of 2PC.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PrepareCommand extends AbstractTransactionBoundaryCommand {

   private static final Log log = LogFactory.getLog(PrepareCommand.class);
   private boolean trace = log.isTraceEnabled();

   public static final byte COMMAND_ID = 12;

   protected WriteCommand[] modifications;
   protected boolean onePhaseCommit;
   protected CacheNotifier notifier;
   protected RecoveryManager recoveryManager;
   private transient boolean replayEntryWrapping  = false;
   
   private static final WriteCommand[] EMPTY_WRITE_COMMAND_ARRAY = new WriteCommand[0];
   private static final Object[] EMPTY_ARRAY = new Object[0];
   private static final Comparator<Object> KEY_COMPARATOR = new Comparator<Object>() {

      private final Hash hash = new MurmurHash3();

      @Override
      public int compare(Object o1, Object o2) {
         return Integer.valueOf(hash.hash(o1)).compareTo(hash.hash(o2));
      }
   };

   public void initialize(CacheNotifier notifier, RecoveryManager recoveryManager) {
      this.notifier = notifier;
      this.recoveryManager = recoveryManager;
   }

   private PrepareCommand() {
      super(null); // For command id uniqueness test
   }

   public PrepareCommand(String cacheName, GlobalTransaction gtx, boolean onePhaseCommit, WriteCommand... modifications) {
      super(cacheName);
      this.globalTx = gtx;
      this.modifications = modifications;
      this.onePhaseCommit = onePhaseCommit;
   }

   public PrepareCommand(String cacheName, GlobalTransaction gtx, List<WriteCommand> commands, boolean onePhaseCommit) {
      super(cacheName);
      this.globalTx = gtx;
      this.modifications = commands == null || commands.isEmpty() ? null : commands.toArray(new WriteCommand[commands.size()]);
      this.onePhaseCommit = onePhaseCommit;
   }

   public PrepareCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public Object perform(InvocationContext ignored) throws Throwable {
      if (ignored != null)
         throw new IllegalStateException("Expected null context!");

      if (recoveryManager != null && recoveryManager.isTransactionPrepared(globalTx)) {
         log.tracef("The transaction %s is already prepared. Skipping prepare call.", globalTx);
         return null;
      }

      // 1. first create a remote transaction (or get the existing one)
      RemoteTransaction remoteTransaction = getRemoteTransaction();
      //set the list of modifications anyway, as the transaction might have already been created by a previous
      //LockControlCommand with null modifications.
      if (hasModifications()) {
         remoteTransaction.setModifications(Arrays.asList(modifications));
      }

      // 2. then set it on the invocation context
      RemoteTxInvocationContext ctx = icf.createRemoteTxInvocationContext(remoteTransaction, getOrigin());

      if (trace)
         log.tracef("Invoking remotely originated prepare: %s with invocation context: %s", this, ctx);
      notifier.notifyTransactionRegistered(ctx.getGlobalTransaction(), false);
      return invoker.invoke(ctx, this);
   }

   @Override
   protected RemoteTransaction getRemoteTransaction() {
      return txTable.getOrCreateRemoteTransaction(globalTx, modifications);
   }

   @Override
   public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
      return visitor.visitPrepareCommand((TxInvocationContext) ctx, this);
   }

   public WriteCommand[] getModifications() {
      return modifications == null ? EMPTY_WRITE_COMMAND_ARRAY : modifications;
   }

   public boolean isOnePhaseCommit() {
      return onePhaseCommit;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      int numMods = modifications == null ? 0 : modifications.length;
      int i = 0;
      final int params = 3;
      Object[] retval = new Object[numMods + params];
      retval[i++] = globalTx;
      retval[i++] = onePhaseCommit;
      retval[i++] = numMods;
      if (numMods > 0) System.arraycopy(modifications, 0, retval, params, numMods);
      return retval;
   }

   @Override
   @SuppressWarnings("unchecked")
   public void setParameters(int commandId, Object[] args) {
      int i = 0;
      globalTx = (GlobalTransaction) args[i++];
      onePhaseCommit = (Boolean) args[i++];
      int numMods = (Integer) args[i++];
      if (numMods > 0) {
         modifications = new WriteCommand[numMods];
         System.arraycopy(args, i, modifications, 0, numMods);
      }
   }

   public PrepareCommand copy() {
      PrepareCommand copy = new PrepareCommand(cacheName);
      copy.globalTx = globalTx;
      copy.modifications = modifications == null ? null : modifications.clone();
      copy.onePhaseCommit = onePhaseCommit;
      return copy;
   }

   @Override
   public String toString() {
      return "PrepareCommand {" +
            "modifications=" + (modifications == null ? null : Arrays.asList(modifications)) +
            ", onePhaseCommit=" + onePhaseCommit +
            ", " + super.toString();
   }

   public boolean hasModifications() {
      return modifications != null && modifications.length > 0;
   }

   public Set<Object> getAffectedKeys() {
      if (modifications == null || modifications.length == 0)
         return InfinispanCollections.emptySet();

      if (modifications.length == 1) return modifications[0].getAffectedKeys();
      Set<Object> keys = new HashSet<Object>(modifications.length);
      for (WriteCommand wc: modifications) keys.addAll(wc.getAffectedKeys());
      return keys;
   }

   /**
    * If set to true, then the keys touched by this transaction are to be wrapped again and original ones discarded.
    */
   public boolean isReplayEntryWrapping() {
      return replayEntryWrapping;
   }

   /**
    * @see #isReplayEntryWrapping()
    */
   public void setReplayEntryWrapping(boolean replayEntryWrapping) {
      this.replayEntryWrapping = replayEntryWrapping;
   }

   public boolean writesToASingleKey() {
      if (modifications == null || modifications.length != 1)
         return false;
      WriteCommand wc = modifications[0];
      return wc instanceof PutKeyValueCommand || wc instanceof RemoveCommand || wc instanceof ReplaceCommand;
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   /**
    * It returns an array of keys affected by the WriteCommand in modifications.
    *
    * @param sort          if {@code true}, the array returned is sorted by the key hash.
    * @return  an array of keys
    */
   public Object[] getAffectedKeysToLock(boolean sort) {
      if (modifications == null) {
         return EMPTY_ARRAY;
      }
      Set<Object> set = new HashSet<Object>(modifications.length);
      for (WriteCommand wc : modifications) {
         switch (wc.getCommandId()) {
            case ClearCommand.COMMAND_ID:
               return null;
            case PutKeyValueCommand.COMMAND_ID:
            case RemoveCommand.COMMAND_ID:
            case ReplaceCommand.COMMAND_ID:
               set.add(((DataWriteCommand) wc).getKey());
               break;
            case PutMapCommand.COMMAND_ID:
               set.addAll(wc.getAffectedKeys());
               break;
            case ApplyDeltaCommand.COMMAND_ID:
               ApplyDeltaCommand command = (ApplyDeltaCommand) wc;
               Object[] compositeKeys = command.getCompositeKeys();
               set.addAll(Arrays.asList(compositeKeys));
               break;
         }
      }
      Object[] sorted = set.toArray(new Object[set.size()]);
      if (sort) {
         TimSort.sort(sorted, KEY_COMPARATOR);
      }
      return sorted;
   }
}
