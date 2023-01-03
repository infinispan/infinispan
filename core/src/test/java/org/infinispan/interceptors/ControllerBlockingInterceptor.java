package org.infinispan.interceptors;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.remoting.inboundhandler.BlockHandler;
import org.infinispan.remoting.inboundhandler.BlockHandlerImpl;

public class ControllerBlockingInterceptor extends BaseAsyncInterceptor {

   public static final Predicate<VisitableCommand> PUT_FOR_STATE_TRANSFER = cmd -> cmd instanceof PutKeyValueCommand && ((PutKeyValueCommand) cmd).hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER);

   private final List<BlockHandlerImpl<VisitableCommand>> commandsToBlock = new CopyOnWriteArrayList<>();

   public static ControllerBlockingInterceptor addBefore(ConfigurationBuilder builder, Class<? extends AsyncInterceptor> before) {
      ControllerBlockingInterceptor interceptor = new ControllerBlockingInterceptor();
      builder.customInterceptors()
            .addInterceptor()
            .before(before)
            .interceptor(interceptor);
      return interceptor;
   }

   public static ControllerBlockingInterceptor addAfter(ConfigurationBuilder builder, Class<? extends AsyncInterceptor> after) {
      ControllerBlockingInterceptor interceptor = new ControllerBlockingInterceptor();
      builder.customInterceptors()
            .addInterceptor()
            .after(after)
            .interceptor(interceptor);
      return interceptor;
   }

   @Override
   public Object visitCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
      for (BlockHandlerImpl<VisitableCommand> block : commandsToBlock) {
         if (block.test(command)) {
            block.onBlocked();
            return makeStage(asyncInvokeNext(ctx, command, block.blockingFuture()))
                  .andFinallyMakeStage(ctx, command, (rCtx, rCommand, rv, throwable) -> block.onFinished());
         }
      }
      return invokeNext(ctx, command);
   }

   public BlockHandler blockCommand(Class<? extends VisitableCommand> commandClass) {
      return blockCommand(commandClass::isInstance);
   }

   public BlockHandler blockCommand(Predicate<VisitableCommand> commandPredicate) {
      BlockHandlerImpl<VisitableCommand> impl = new BlockHandlerImpl<>(commandPredicate);
      commandsToBlock.add(impl);
      return impl;
   }
}
