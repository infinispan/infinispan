package org.infinispan.test.concurrent;

import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;

import java.util.List;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.responses.ExceptionResponse;

/**
 * Replaces the {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler} with a wrapper that can interact with a {@link StateSequencer} when a
 * command that matches a {@link CommandMatcher} is invoked.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class InboundRpcSequencerAction {
   private final StateSequencer stateSequencer;
   private final Cache<?,?> cache;
   private final CommandMatcher matcher;
   private SequencerPerCacheInboundInvocationHandler ourHandler;

   public InboundRpcSequencerAction(StateSequencer stateSequencer, Cache cache, CommandMatcher matcher) {
      this.stateSequencer = stateSequencer;
      this.cache = cache;
      this.matcher = matcher;
   }

   /**
    * Set up a list of sequencer states before interceptor {@code interceptorClass} is called.
    * <p/>
    * Each invocation accepted by {@code matcher} will enter/exit the next state from the list, and does nothing after the list is exhausted.
    */
   public InboundRpcSequencerAction before(String state1, String... additionalStates) {
      replaceInboundInvocationHandler();
      ourHandler.beforeStates(StateSequencerUtil.concat(state1, additionalStates));
      return this;
   }

   private void replaceInboundInvocationHandler() {
      if (ourHandler == null) {
         ourHandler = wrapInboundInvocationHandler(cache, handler ->
               new SequencerPerCacheInboundInvocationHandler(handler, stateSequencer, matcher));
      }
   }

   /**
    * Set up a list of sequencer states after interceptor {@code interceptorClass} has returned.
    * <p/>
    * Each invocation accepted by {@code matcher} will enter/exit the next state from the list, and does nothing after the list is exhausted.
    */
   public InboundRpcSequencerAction after(String state1, String... additionalStates) {
      replaceInboundInvocationHandler();
      ourHandler.afterStates(StateSequencerUtil.concat(state1, additionalStates));
      return this;
   }

   public static class SequencerPerCacheInboundInvocationHandler extends AbstractDelegatingHandler {
      private final StateSequencer stateSequencer;
      private final CommandMatcher matcher;
      private volatile List<String> statesBefore;
      private volatile List<String> statesAfter;

      public SequencerPerCacheInboundInvocationHandler(PerCacheInboundInvocationHandler delegate, StateSequencer stateSequencer, CommandMatcher matcher) {
         super(delegate);
         this.stateSequencer = stateSequencer;
         this.matcher = matcher;
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         boolean accepted = matcher.accept(command);
         advance(accepted, statesBefore, reply);
         try {
            delegate.handle(command, reply, order);
         } finally {
            advance(accepted, statesAfter, Reply.NO_OP);
         }
      }

      public void beforeStates(List<String> states) {
         this.statesBefore = StateSequencerUtil.listCopy(states);
      }

      public void afterStates(List<String> states) {
         this.statesAfter = StateSequencerUtil.listCopy(states);
      }

      private void advance(boolean accepted, List<String> states, Reply reply) {
         try {
            StateSequencerUtil.advanceMultiple(stateSequencer, accepted, states);
         } catch (TimeoutException e) {
            reply.reply(new ExceptionResponse(e));
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            reply.reply(new ExceptionResponse(e));
         }
      }
   }
}
