package org.infinispan.test.concurrent;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.CommandAwareRpcDispatcher;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.test.TestingUtil;
import org.jgroups.blocks.Response;

import java.util.List;

/**
 * Replaces the {@link InboundInvocationHandler} with a wrapper that can interact with a {@link StateSequencer} when a
 * command that matches a {@link CommandMatcher} is invoked.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class InboundRpcSequencerAction {
   private final StateSequencer stateSequencer;
   private final EmbeddedCacheManager cacheManager;
   private final CommandMatcher matcher;
   private SequencerInboundInvocationHandler ourHandler;

   public InboundRpcSequencerAction(StateSequencer stateSequencer, EmbeddedCacheManager cacheManager, CommandMatcher matcher) {
      this.stateSequencer = stateSequencer;
      this.cacheManager = cacheManager;
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
         GlobalComponentRegistry globalComponentRegistry = cacheManager.getGlobalComponentRegistry();
         InboundInvocationHandler handler = globalComponentRegistry.getComponent(InboundInvocationHandler.class);
         ourHandler = new SequencerInboundInvocationHandler(handler, stateSequencer, matcher);
         TestingUtil.replaceComponent(cacheManager, InboundInvocationHandler.class, ourHandler, true);
         JGroupsTransport t = (JGroupsTransport) globalComponentRegistry.getComponent(Transport.class);
         CommandAwareRpcDispatcher card = t.getCommandAwareRpcDispatcher();
         TestingUtil.replaceField(ourHandler, "inboundInvocationHandler", card, CommandAwareRpcDispatcher.class);
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

   public static class SequencerInboundInvocationHandler implements InboundInvocationHandler {
      private final StateSequencer stateSequencer;
      private final CommandMatcher matcher;
      private final InboundInvocationHandler handler;
      private volatile List<String> statesBefore;
      private volatile List<String> statesAfter;

      public SequencerInboundInvocationHandler(InboundInvocationHandler handler, StateSequencer stateSequencer, CommandMatcher matcher) {
         this.handler = handler;
         this.stateSequencer = stateSequencer;
         this.matcher = matcher;
      }


      @Override
      public void handle(CacheRpcCommand command, Address origin, Response response, boolean preserveOrder) throws Throwable {
         // Normally InboundInvocationHandlerImpl does this, but we want matchers to have access to the origin
         command.setOrigin(origin);

         boolean accepted = matcher.accept(command);
         StateSequencerUtil.advanceMultiple(stateSequencer, accepted, statesBefore);
         try {
            handler.handle(command, origin, response, preserveOrder);
         } finally {
            StateSequencerUtil.advanceMultiple(stateSequencer, accepted, statesAfter);
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
