package org.infinispan.lucene;

import org.infinispan.commands.AbstractFlagAffectedCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * SkipIndexingGuaranteed is an interceptor to verify all write operations are using the org.infinispan.context.Flag.SKIP_INDEXING
 * Using SKIP_INDEXING is much lighter than having Infinispan Query need to use class reflection and attempt to reconfigure the Search engine
 * dynamically.
 *
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2012 Red Hat Inc.
 * @since 5.2
 */
public class SkipIndexingGuaranteed extends CommandInterceptor {

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleDefaultCheckingAssertion(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDefaultCheckingAssertion(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDefaultCheckingAssertion(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleDefaultCheckingAssertion(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleDefaultCheckingAssertion(ctx, command);
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      return handleDefaultCheckingAssertion(ctx, command);
   }

   protected Object handleDefaultCheckingAssertion(InvocationContext ctx, AbstractFlagAffectedCommand command) throws Throwable {
      if (! command.hasFlag(Flag.SKIP_INDEXING)) {
         throw new AssertionError("A write operation was detected which is not using SKIP_INDEXING flag");
      }
      return super.invokeNextInterceptor(ctx, command);
   }

}
