package org.infinispan.test.concurrent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;


/**
 * Various helper methods for working with {@link StateSequencer}s.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class StateSequencerUtil {
   /**
    * Start decorating interceptor {@code interceptorClass} on {@code cache} to interact with a {@code StateSequencer}.
    */
   public static InterceptorSequencerAction advanceOnInterceptor(StateSequencer stateSequencer, Cache<?, ?> cache,
         Class<? extends AsyncInterceptor> interceptorClass, CommandMatcher matcher) {
      return new InterceptorSequencerAction(stateSequencer, cache, interceptorClass, matcher);
   }

   /**
    * Start decorating the {@code InboundInvocationHandler} on {@code cacheManager} to interact with a {@code StateSequencer}
    * when a {@code CacheRpcCommand} is received.
    */
   public static InboundRpcSequencerAction advanceOnInboundRpc(StateSequencer stateSequencer, Cache cache,
         CommandMatcher matcher) {
      return new InboundRpcSequencerAction(stateSequencer, cache, matcher);
   }

   /**
    * Start decorating the {@code RpcManager} on {@code cacheManager} to interact with a {@code StateSequencer} when a
    * command is sent.
    */
   public static OutboundRpcSequencerAction advanceOnOutboundRpc(StateSequencer stateSequencer, Cache<?, ?> cache,
         CommandMatcher matcher) {
      return new OutboundRpcSequencerAction(stateSequencer, cache, matcher);
   }

   /**
    * Start decorating the component {@code componentClass} on {@code cache} to interact with a {@code StateSequencer} when a
    * method is called.
    */
   public static CacheComponentSequencerAction advanceOnComponentMethod(StateSequencer stateSequencer, Cache<?, ?> cache, Class<?> componentClass,
         InvocationMatcher matcher) {
      return new CacheComponentSequencerAction(stateSequencer, cache, componentClass, matcher);
   }

   /**
    * Start decorating the component {@code componentClass} on {@code cacheManager} to interact with a {@code StateSequencer}
    * when a method is called.
    */
   public static <T> GlobalComponentSequencerAction<T> advanceOnGlobalComponentMethod(StateSequencer stateSequencer,
         EmbeddedCacheManager cacheManager, Class<T> componentClass, InvocationMatcher matcher) {
      return new GlobalComponentSequencerAction(stateSequencer, cacheManager, componentClass, matcher);
   }

   /**
    * Start building a {@link CommandMatcher}.
    */
   public static CommandMatcherBuilder matchCommand(Class<? extends ReplicableCommand> commandClass) {
      return new CommandMatcherBuilder(commandClass);
   }

   /**
    * Start building a {@link InvocationMatcher}.
    */
   public static InvocationMatcherBuilder matchMethodCall(String methodName) {
      return new InvocationMatcherBuilder(methodName);
   }

   public static List<String> listCopy(List<String> statesUp) {
      return statesUp != null ? Collections.unmodifiableList(new LinkedList<String>(statesUp)) : null;
   }

   public static List<String> concat(String state1, String... additionalStates) {
      List<String> states = new ArrayList<String>();
      states.add(state1);
      if (additionalStates != null) {
         states.addAll(Arrays.asList(additionalStates));
      }
      return states;
   }

   /**
    * Advance to the every state in the {@code states} list, in the given order, but only if {@code condition} is true.
    * <p/>
    * Does nothing if {@code states} is {@code null} or empty.
    */
   public static void advanceMultiple(StateSequencer stateSequencer, boolean condition, List<String> states)
         throws TimeoutException, InterruptedException {
      if (condition && states != null) {
         for (String state : states) {
            stateSequencer.advance(state);
         }
      }
   }
}
