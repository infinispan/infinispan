package org.infinispan.test.concurrent;

import java.util.Collection;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.AbstractControlledRpcManager;

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

   public static class SequencerRpcManager extends AbstractControlledRpcManager {
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
      protected Object beforeInvokeRemotely(Collection<Address> targets, ReplicableCommand command) {
         try {
            boolean accept = matcher.accept(command);
            StateSequencerUtil.advanceMultiple(stateSequencer, accept, statesBefore);
            return accept;
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }

      @Override
      protected <T> T afterInvokeRemotely(Collection<Address> targets, ReplicableCommand command, T responseObject, Object argument) {
         try {
            StateSequencerUtil.advanceMultiple(stateSequencer, (Boolean) argument, statesAfter);
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
         return responseObject;
      }

      public void beforeStates(List<String> states) {
         this.statesBefore = StateSequencerUtil.listCopy(states);
      }

      public void afterStates(List<String> states) {
         this.statesAfter = StateSequencerUtil.listCopy(states);
      }
   }
}
