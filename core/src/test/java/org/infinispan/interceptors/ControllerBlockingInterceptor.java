package org.infinispan.interceptors;

import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;

public class ControllerBlockingInterceptor extends BaseAsyncInterceptor {

   public static final Predicate<VisitableCommand> PUT_FOR_STATE_TRANSFER = cmd -> cmd instanceof PutKeyValueCommand && ((PutKeyValueCommand) cmd).hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER);

   private final List<CommandControllerImpl> commandsToBlock = new CopyOnWriteArrayList<>();

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
      for (CommandControllerImpl controller : commandsToBlock) {
         if (controller.commandPredicate.test(command)) {
            controller.blocked.countDown();
            return asyncInvokeNext(ctx, command, controller.proceed);
         }
      }
      return invokeNext(ctx, command);
   }

   public CommandController blockCommand(Class<? extends VisitableCommand> commandClass) {
      return blockCommand(commandClass::isInstance);
   }

   public CommandController blockCommand(Predicate<VisitableCommand> commandPredicate) {
      CommandControllerImpl impl = new CommandControllerImpl(commandPredicate);
      commandsToBlock.add(impl);
      return impl;
   }

   static class CommandControllerImpl implements CommandController {

      final Predicate<VisitableCommand> commandPredicate;
      final CountDownLatch blocked = new CountDownLatch(1);
      final CompletableFuture<Void> proceed = new CompletableFuture<>();

      CommandControllerImpl(Predicate<VisitableCommand> commandPredicate) {
         this.commandPredicate = commandPredicate;
      }

      @Override
      public void awaitCommandBlocked() throws InterruptedException {
         assertTrue(blocked.await(10, TimeUnit.SECONDS));
      }

      @Override
      public void unblockCommand() {
         proceed.complete(null);
      }
   }
}
