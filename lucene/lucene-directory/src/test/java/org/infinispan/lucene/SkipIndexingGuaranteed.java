package org.infinispan.lucene;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.interceptors.base.CommandInterceptor;

/**
 * SkipIndexingGuaranteed is an interceptor to verify all write operations are using the org.infinispan.context.Flag.SKIP_INDEXING
 * Using SKIP_INDEXING is much lighter than having Infinispan Query need to use class reflection and attempt to reconfigure the Search engine
 * dynamically.
 *
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2012 Red Hat Inc.
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
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleDefaultCheckingAssertion(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
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

   protected Object handleDefaultCheckingAssertion(InvocationContext ctx, FlagAffectedCommand command) throws Throwable {
      if (! command.hasAnyFlag(FlagBitSets.SKIP_INDEXING)) {
         throw new AssertionError("A write operation was detected which is not using SKIP_INDEXING flag");
      }
      return super.invokeNextInterceptor(ctx, command);
   }

}
