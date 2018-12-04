package org.infinispan.test.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.AbstractDelegatingRpcManager;

/**
 * Replaces the {@link RpcManager} with a wrapper that can interact with a {@link StateSequencer} when a
 * command that matches a {@link CommandMatcher} is invoked remotely.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class OutboundRpcSequencerAction {
   private final StateSequencer stateSequencer;
   private final Cache<?, ?> cache;
   private final CommandMatcher matcher;
   private SequencerRpcManager ourRpcManager;

   public OutboundRpcSequencerAction(StateSequencer stateSequencer, Cache<?, ?> cache, CommandMatcher matcher) {
      this.stateSequencer = stateSequencer;
      this.cache = cache;
      this.matcher = matcher;
   }

   /**
    * Set up a list of sequencer states before interceptor {@code interceptorClass} is called.
    * <p/>
    * Each invocation accepted by {@code matcher} will enter/exit the next state from the list, and does nothing after the list is exhausted.
    */
   public OutboundRpcSequencerAction before(String state1, String... additionalStates) {
      replaceRpcManager();
      ourRpcManager.beforeStates(StateSequencerUtil.concat(state1, additionalStates));
      return this;
   }

   private void replaceRpcManager() {
      if (ourRpcManager == null) {
         ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
         RpcManager rpcManager = componentRegistry.getComponent(RpcManager.class);
         ourRpcManager = new SequencerRpcManager(rpcManager, stateSequencer, matcher);
         TestingUtil.replaceComponent(cache, RpcManager.class, ourRpcManager, true);
      }
   }

   /**
    * Set up a list of sequencer states after interceptor {@code interceptorClass} has returned.
    * <p/>
    * Each invocation accepted by {@code matcher} will enter/exit the next state from the list, and does nothing after the list is exhausted.
    */
   public OutboundRpcSequencerAction after(String state1, String... additionalStates) {
      replaceRpcManager();
      ourRpcManager.afterStates(StateSequencerUtil.concat(state1, additionalStates));
      return this;
   }

   public static class SequencerRpcManager extends AbstractDelegatingRpcManager {
      private final StateSequencer stateSequencer;
      private final CommandMatcher matcher;
      private volatile List<String> statesBefore;
      private volatile List<String> statesAfter;

      public SequencerRpcManager(RpcManager rpcManager, StateSequencer stateSequencer, CommandMatcher matcher) {
         super(rpcManager);
         this.stateSequencer = stateSequencer;
         this.matcher = matcher;
      }

      @Override
      protected <T> CompletionStage<T> performRequest(Collection<Address> targets, ReplicableCommand command,
                                                      ResponseCollector<T> collector,
                                                      Function<ResponseCollector<T>, CompletionStage<T>>
                                                         invoker, RpcOptions rpcOptions) {
         boolean accept;
         try {
            accept = matcher.accept(command);
            StateSequencerUtil.advanceMultiple(stateSequencer, accept, statesBefore);
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
         CompletionStage<T> stage = super.performRequest(targets, command, collector, invoker, rpcOptions);
         if (stage != null) {
            return stage.whenComplete((result, throwable) -> advanceNoThrow(accept));
         } else {
            advanceNoThrow(accept);
            return null;
         }
      }

      private void advanceNoThrow(boolean accept) {
         try {
            StateSequencerUtil.advanceMultiple(stateSequencer, accept, statesAfter);
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }

      public void beforeStates(List<String> states) {
         this.statesBefore = StateSequencerUtil.listCopy(states);
      }

      public void afterStates(List<String> states) {
         this.statesAfter = StateSequencerUtil.listCopy(states);
      }
   }
}
