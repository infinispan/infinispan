package org.infinispan.test.concurrent;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.DDSequentialInterceptor;
import org.infinispan.interceptors.SequentialInterceptor;
import org.infinispan.interceptors.SequentialInterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Replaces a {@link CommandInterceptor} with a wrapper that can interact with a {@link StateSequencer} when a
 * command that matches a {@link CommandMatcher} is visited.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class InterceptorSequencerAction {
   private final StateSequencer stateSequencer;
   private final Cache<?, ?> cache;
   private final Class<? extends SequentialInterceptor> interceptorClass;
   private CommandMatcher matcher;
   private SequencerInterceptor ourInterceptor;

   public InterceptorSequencerAction(StateSequencer stateSequencer, Cache<?, ?> cache, Class<? extends SequentialInterceptor> interceptorClass, CommandMatcher matcher) {
      this.stateSequencer = stateSequencer;
      this.cache = cache;
      this.interceptorClass = interceptorClass;
      this.matcher = matcher;
   }

   /**
    * Set up a list of sequencer states before interceptor {@code interceptorClass} is called.
    */
   public InterceptorSequencerAction before(String state1, String... additionalStates) {
      initOurInterceptor();
      ourInterceptor.beforeStates(StateSequencerUtil.concat(state1, additionalStates));
      return this;
   }

   // TODO Should we add beforeInvokeNext() and afterInvokeNext()?

   private void initOurInterceptor() {
      if (ourInterceptor == null) {
         ourInterceptor = SequencerInterceptor.createUniqueInterceptor(cache.getAdvancedCache().getSequentialInterceptorChain());
         ourInterceptor.init(stateSequencer, matcher);
         cache.getAdvancedCache().getSequentialInterceptorChain().addInterceptorBefore(ourInterceptor, interceptorClass);
      }
   }

   /**
    * Set up a list of sequencer states after interceptor {@code interceptorClass} has returned.
    * <p/>
    * Each invocation accepted by {@code matcher} will enter/exit the next state from the list, and does nothing after the list is exhausted.
    */
   public InterceptorSequencerAction after(String state1, String... additionalStates) {
      initOurInterceptor();
      ourInterceptor.afterStates(StateSequencerUtil.concat(state1, additionalStates));
      return this;
   }

   public static class SequencerInterceptor extends DDSequentialInterceptor {
      private static final Class[] uniqueInterceptorClasses = {
            U1.class, U2.class, U3.class, U4.class, U5.class, U6.class, U7.class, U8.class, U9.class
      };

      private StateSequencer stateSequencer;
      private CommandMatcher matcher;
      private volatile List<String> statesBefore;
      private volatile List<String> statesAfter;

      public static SequencerInterceptor createUniqueInterceptor(SequentialInterceptorChain chain) {
         Class uniqueClass = findUniqueClass(chain);
         try {
            return (SequencerInterceptor) uniqueClass.newInstance();
         } catch (Exception e) {
            throw new RuntimeException("Cannot instantiate unique interceptor", e);
         }
      }

      public static Class<?> findUniqueClass(SequentialInterceptorChain chain) {
         for (Class<? extends SequentialInterceptor> clazz : uniqueInterceptorClasses) {
            if (!chain.containsInterceptorType(clazz)) {
               return clazz;
            }
         }

         throw new IllegalStateException("Too many sequencer interceptors added to the same chain");
      }

      public void init(StateSequencer stateSequencer, CommandMatcher matcher) {
         this.stateSequencer = stateSequencer;
         this.matcher = matcher;
      }

      @Override
      protected CompletableFuture<Void> handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
         boolean commandAccepted = matcher.accept(command);
         StateSequencerUtil.advanceMultiple(stateSequencer, commandAccepted, statesBefore);
         try {
            return ctx.shortCircuit(ctx.forkInvocationSync(command));
         } finally {
            StateSequencerUtil.advanceMultiple(stateSequencer, commandAccepted, statesAfter);
         }
      }

      public void beforeStates(List<String> states) {
         this.statesBefore = StateSequencerUtil.listCopy(states);
      }

      public void afterStates(List<String> states) {
         this.statesAfter = StateSequencerUtil.listCopy(states);
      }

      public static class U1 extends SequencerInterceptor {
      }

      public static class U2 extends SequencerInterceptor {
      }

      public static class U3 extends SequencerInterceptor {
      }

      public static class U4 extends SequencerInterceptor {
      }

      public static class U5 extends SequencerInterceptor {
      }

      public static class U6 extends SequencerInterceptor {
      }

      public static class U7 extends SequencerInterceptor {
      }

      public static class U8 extends SequencerInterceptor {
      }

      public static class U9 extends SequencerInterceptor {
      }
   }
}
