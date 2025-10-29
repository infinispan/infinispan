package org.infinispan.distribution;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.test.Mocks;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor that allows for waiting for a command to be invoked and either delaying before or after the completes,
 * but in a non blocking fashion. This interceptor is very similar to {@link BlockingInterceptor} and should be able
 * to be dropped in replacement.
 * <p>
 * The checkPoint uses {@link Mocks} before and after invocation/release operations to determine what the named
 * check points are.
 *
 * @author William Burns
 * @since 16.0
 */
public class CheckPointInterceptor<T extends VisitableCommand> extends DDAsyncInterceptor {
   private static final Log log = LogFactory.getLog(CheckPointInterceptor.class);

   private final CheckPoint checkPoint;
   private final boolean blockAfter;
   private final boolean originLocalOnly;
   private final AtomicBoolean suspended = new AtomicBoolean();
   private final Predicate<VisitableCommand> acceptCommand;
   private final Executor executorToUse;

   public CheckPointInterceptor(CheckPoint checkPoint, Executor executorToUse, Class<T> commandClass,
                                boolean blockAfter, boolean originLocalOnly) {
      this(checkPoint, executorToUse, commandClass, blockAfter, originLocalOnly, t -> t != null && commandClass == t.getClass());
   }

   public CheckPointInterceptor(CheckPoint checkPoint, Executor executorToUse, Class<T> commandClass,
                                boolean blockAfter, boolean originLocalOnly, Predicate<T> acceptCommand) {
      this(checkPoint, executorToUse, blockAfter, originLocalOnly,
            t -> t != null && commandClass == t.getClass() && acceptCommand.test(commandClass.cast(t)));
   }

   public CheckPointInterceptor(CheckPoint checkPoint, Executor executorToUse, boolean blockAfter, boolean originLocalOnly,
                                Predicate<VisitableCommand> acceptCommand) {
      this.checkPoint = Objects.requireNonNull(checkPoint);
      this.blockAfter = blockAfter;
      this.originLocalOnly = originLocalOnly;
      this.acceptCommand = Objects.requireNonNull(acceptCommand);
      this.executorToUse = Objects.requireNonNull(executorToUse);
   }

   public void suspend() {
      this.suspended.set(true);
      checkPoint.triggerForever(Mocks.BEFORE_RELEASE);
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);
   }

   private boolean canHandle(InvocationContext ctx, VisitableCommand command) {
      return (!originLocalOnly || ctx.isOriginLocal()) && acceptCommand.test(command);
   }

   @Override
   protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
      if (suspended.get()) {
         log.tracef("Suspended, not blocking command %s", command);
         return invokeNext(ctx, command);
      }
      Object val;
      if (!blockAfter && canHandle(ctx, command)) {
         log.tracef("Command blocking %s completion of before", command);
         checkPoint.trigger(Mocks.BEFORE_INVOCATION);
         val = asyncInvokeNext(ctx, command, checkPoint.future(Mocks.BEFORE_RELEASE, 10, TimeUnit.SECONDS, executorToUse));
      } else {
         log.tracef("Not blocking command %s", command);
         val = invokeNext(ctx, command);
      }
      if (blockAfter && canHandle(ctx, command)) {
         log.tracef("Command blocking %s completion of after", command);
         checkPoint.trigger(Mocks.AFTER_INVOCATION);
         return makeStage(val).thenApplyMakeStage(ctx, command,
               (rCtx, rCmd, rVal) -> delayedValue(checkPoint.future(Mocks.AFTER_RELEASE, 10, TimeUnit.SECONDS, executorToUse), rVal));
      }
      log.tracef("Not blocking command %s", command);
      return val;
   }
}
