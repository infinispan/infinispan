package org.infinispan.interceptors.base;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;

import java.util.function.BiFunction;

/**
 * An interceptor in the new sequential invocation chain.
 * 
 * @author Dan Berindei
 * @since 8.1
 */
public interface SequentialInterceptor extends Visitor {
   /**
    * Execute the interceptor before {@code command.perform()}.
    *
    * The interceptor must return an instance of {@code BiFunction<Object, Throwable, Object>},
    * or {@code null} if it doesn't need to perform any work after {@code command.perform()}.
    * If it needs to skip the execution of the rest of the chain (and the command itself),
    * the interceptor must use {@code return skipNextInterceptor(earlyReturnValue)}.
    * If it needs to invoke a new command, starting with the next interceptor in the chain,
    * the interceptor must use {@code return invokeNewCommand(command)}.
    */
   default BiFunction<Object, Throwable, Object> visitCommand(InvocationContext ctx, VisitableCommand cmd) throws Throwable {
      return (BiFunction<Object, Throwable, Object>) cmd.acceptVisitor(ctx, this);
   }

   BiFunction<Object, Throwable, Object> skipNextInterceptor(Object value);

   BiFunction<Object, Throwable, Object> invokeNewCommand(VisitableCommand cmd);

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitInvalidateCommand(InvocationContext ctx, InvalidateCommand invalidateCommand)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command invalidateL1Command)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitUnknownCommand(InvocationContext ctx, VisitableCommand command) throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   <V> BiFunction<Object, Throwable, Object> visitDistributedExecuteCommand(InvocationContext ctx,
                                                     DistributedExecuteCommand<V> command) throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command)
         throws Throwable;

   /**
    * @see #visitCommand(InvocationContext, VisitableCommand)
    */
   @Override
   BiFunction<Object, Throwable, Object> visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command)
         throws Throwable;

   class SkipNextInterceptor implements BiFunction<Object, Throwable, Object> {
      public final Object returnValue;

      public SkipNextInterceptor(Object returnValue) {
         this.returnValue = returnValue;
      }

      @Override
      public Object apply(Object o, Throwable throwable) {
         throw new UnsupportedOperationException();
      }
   }

   class NewCommand implements BiFunction<Object, Throwable, Object> {
      public final VisitableCommand cmd;

      public NewCommand(VisitableCommand cmd) {
         this.cmd = cmd;
      }

      @Override
      public Object apply(Object o, Throwable throwable) {
         throw new UnsupportedOperationException();
      }
   }
}
