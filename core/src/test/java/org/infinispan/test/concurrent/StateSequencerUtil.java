package org.infinispan.test.concurrent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.interceptors.base.CommandInterceptor;


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
         Class<? extends CommandInterceptor> interceptorClass, CommandMatcher matcher) {
      return new InterceptorSequencerAction(stateSequencer, cache, interceptorClass, matcher);
   }

   /**
    * Start building a {@link CommandMatcher}.
    */
   public static CommandMatcherBuilder matchCommand(Class<? extends ReplicableCommand> commandClass) {
      return new CommandMatcherBuilder(commandClass);
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
