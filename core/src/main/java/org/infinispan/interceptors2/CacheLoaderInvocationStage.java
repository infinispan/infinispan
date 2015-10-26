package org.infinispan.interceptors2;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.LocalFlagAffectedCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.interceptors.CacheLoaderInterceptor;

import java.util.concurrent.CompletableFuture;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public class CacheLoaderInvocationStage extends CacheLoaderInterceptor implements InvocationStage {
   @Override
   public CompletableFuture<Object> beforeCommand(PipelineContext ctx, VisitableCommand2 cmd)
         throws Throwable {
      if (!cmd.needsExistingValues())
         return null;

      if (cmd instanceof FlagAffectedCommand && hasSkipLoadFlag((LocalFlagAffectedCommand) cmd))
         return null;

      if (cmd instanceof DataCommand) {
         if (cmd instanceof InvalidateCommand) {
            for (Object key : ((InvalidateCommand) cmd).getKeys()) {
               loadIfNeeded(ctx.getInvocationContext(), key, (FlagAffectedCommand) cmd);
            }
         } else {
            loadIfNeeded(ctx.getInvocationContext(), ((DataCommand) cmd).getKey(), (FlagAffectedCommand) cmd);
         }
      }
      return null;
   }

   @Override
   public CompletableFuture<Object> afterCommand(PipelineContext ctx, VisitableCommand2 cmd, Object result,
                                                 Throwable exception) {
      return null;
   }
}
