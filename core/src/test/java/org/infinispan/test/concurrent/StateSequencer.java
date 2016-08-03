package org.infinispan.test.concurrent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

/**
 * Defines a set of logical threads, each with a list of states, and a partial ordering between states.
 * <p/>
 * <p>Logical threads are defined with {@link #logicalThread(String, String, String...)}. States in a logical thread are implicitly
 * ordered - they must be entered in the order in which they were defined.</p>
 * <p>The ordering between states in different logical threads can be defined with {@link #order(String, String, String...)}</p>
 * <p>A state can also have an associated action, defined with {@link #action(String, java.util.concurrent.Callable)}.
 * States that depend on another state with an associated action can only be entered after the action has finished.</p>
 * <p>Entering a state with {@link #enter(String)} will block until all the other states it depends on have been exited
 * with {@link #exit(String)}.</p>
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class StateSequencer {
   private static final Log log = LogFactory.getLog(StateSequencer.class);

   private final Map<String, LogicalThread> logicalThreads = new HashMap<String, LogicalThread>();
   private final Map<String, State> stateMap = new HashMap<String, State>();
   private final Lock lock = new ReentrantLock();
   private final Condition condition = lock.newCondition();
   private final long defaultTimeoutNanos;
   private boolean running = true;

   public StateSequencer() {
      this(30, TimeUnit.SECONDS);
   }

   public StateSequencer(long defaultTimeout, TimeUnit unit) {
      this.defaultTimeoutNanos = unit.toNanos(defaultTimeout);
   }

   /**
    * Define a logical thread.
    * <p/>
    * States in a logical thread are implicitly ordered - they must be entered in the order in which they were defined.
    */
   public StateSequencer logicalThread(String threadName, String initialState, String... additionalStates) {
      lock.lock();
      try {
         if (logicalThreads.containsKey(threadName)) {
            throw new IllegalArgumentException("Logical thread " + threadName + " already exists");
         }
         List<String> states;
         if (additionalStates == null) {
            states = Collections.singletonList(initialState);
         } else {
            states = new ArrayList<String>(additionalStates.length + 1);
            states.add(initialState);
            states.addAll(Arrays.asList(additionalStates));
         }
         LogicalThread thread = new LogicalThread(threadName, states);
         logicalThreads.put(threadName, thread);

         for (String stateName : states) {
            if (stateMap.containsKey(stateName)) {
               throw new IllegalArgumentException("State " + stateName + " already exists");
            }
            State state = new State(threadName, stateName);
            stateMap.put(stateName, state);
         }
         doOrder(states);
         log.tracef("Added logical thread %s, with states %s", threadName, states);
      } finally {
         lock.unlock();
      }
      return this;
   }

   private void doOrder(List<String> orderedStates) {
      lock.lock();
      try {
         for (int i = 0; i < orderedStates.size(); i++) {
            State state = stateMap.get(orderedStates.get(i));
            if (state == null) {
               throw new IllegalArgumentException("Cannot order a non-existing state: " + orderedStates.get(i));
            }
            if (i > 0) {
               state.dependencies.add(orderedStates.get(i - 1));
            }
         }
         verifyCycles();
         log.tracef("Order changed: %s", getOrderString());
      } finally {
         lock.unlock();
      }
   }

   @GuardedBy("lock")
   private void verifyCycles() {
      visitInOrder(new StatesVisitor() {
         @Override
         public void visitStates(List<String> visitedStates) {
            // Do nothing
         }

         @Override
         public void visitCycle(Collection<String> remainingStates) {
            throw new IllegalStateException("Cycle detected: " + remainingStates);
         }
      });
   }

   private String getOrderString() {
      final StringBuilder sb = new StringBuilder();
      visitInOrder(new StatesVisitor() {
         @Override
         public void visitStates(List<String> visitedStates) {
            if (sb.length() > 1) {
               sb.append(" < ");
            }
            if (visitedStates.size() == 1) {
               sb.append(visitedStates.get(0));
            } else {
               sb.append(visitedStates);
            }
         }

         @Override
         public void visitCycle(Collection<String> remainingStates) {
            sb.append("cycle: ").append(remainingStates);
         }
      });
      return sb.toString();
   }

   @GuardedBy("lock")
   private void visitInOrder(StatesVisitor visitor) {
      Set<String> visitedStates = new HashSet<String>();
      Set<String> remainingStates = new HashSet<String>(stateMap.keySet());
      while (!remainingStates.isEmpty()) {
         // In every iteration, we visit the states for which we already visited all their dependencies.
         // If there are no such states, it means we found a cycle.
         List<String> freeStates = new ArrayList<String>();
         for (Iterator<String> it = remainingStates.iterator(); it.hasNext(); ) {
            State s = stateMap.get(it.next());
            if (visitedStates.containsAll(s.dependencies)) {
               freeStates.add(s.name);
               it.remove();
            }
         }
         visitedStates.addAll(freeStates);
         if (freeStates.size() != 0) {
            visitor.visitStates(freeStates);
         } else {
            visitor.visitCycle(remainingStates);
         }
      }
   }

   /**
    * Define a partial order between states in different logical threads.
    */
   public StateSequencer order(String state1, String state2, String... additionalStates) {
      List<String> allStates;
      if (additionalStates == null) {
         allStates = new ArrayList<String>(Arrays.asList(state1, state2));
      } else {
         allStates = new ArrayList<String>(additionalStates.length + 2);
         allStates.add(state1);
         allStates.add(state2);
         allStates.addAll(Arrays.asList(additionalStates));
      }
      doOrder(allStates);
      return this;
   }

   /**
    * Define an action for a state.
    * <p/>
    * States that depend on another state with an associated action can only be entered after the action has finished.
    */
   public StateSequencer action(String stateName, Callable<Object> action) {
      lock.lock();
      try {
         State state = stateMap.get(stateName);
         if (state == null) {
            throw new IllegalArgumentException("Trying to add an action for an invalid state: " + stateName);
         }
         if (state.action != null) {
            throw new IllegalStateException("Trying to overwrite an existing action for state " + stateName);
         }
         state.action = action;
         log.tracef("Action added for state %s", stateName);
      } finally {
         lock.unlock();
      }
      return this;
   }

   /**
    * Equivalent to {@code enter(state, timeout, unit); exit(state);}.
    */
   public void advance(String state, long timeout, TimeUnit unit) throws TimeoutException, InterruptedException {
      enter(state, timeout, unit);
      exit(state);
   }

   /**
    * Enter a state and block until all its dependencies have been exited.
    */
   public void enter(String stateName, long timeout, TimeUnit unit) throws TimeoutException, InterruptedException {
      doEnter(stateName, unit.toNanos(timeout));
   }

   /**
    * Exit a state and signal the waiters on its dependent states.
    */
   public void exit(String stateName) {
      log.tracef("Exiting state %s", stateName);
      lock.lock();
      try {
         if (!running)
            return;

         State state = stateMap.get(stateName);
         if (state.signalled) {
            throw new IllegalStateException(String.format("State %s exited twice", stateName));
         }

         state.signalled = true;
         condition.signalAll();
      } finally {
         lock.unlock();
      }
   }

   private void doEnter(String stateName, long nanos) throws InterruptedException, TimeoutException {
      lock.lock();
      try {
         State state = stateMap.get(stateName);
         if (state == null) {
            throw new IllegalArgumentException("Trying to advance to a non-existing state: " + stateName);
         }
         if (!running) {
            log.tracef("Sequencer stopped, not entering state %s", stateName);
            return;
         }

         log.tracef("Waiting for states %s to enter %s", state.dependencies, stateName);
         for (String dependency : state.dependencies) {
            State depState = stateMap.get(dependency);
            nanos = waitForState(depState, nanos);
            if (nanos <= 0 && !depState.signalled) {
               reportTimeout(state);
            }
         }

         log.tracef("Entering state %s", stateName);
         logicalThreads.get(state.threadName).setCurrentState(stateName);

         if (state.action != null) {
            try {
               state.action.call();
            } catch (Exception e) {
               throw new RuntimeException("Action failed for state " + stateName, e);
            }
         }
      } finally {
         lock.unlock();
      }
   }

   @GuardedBy("lock")
   private long waitForState(State state, long nanos) throws InterruptedException {
      while (running && !state.signalled && nanos > 0L) {
         nanos = condition.awaitNanos(nanos);
      }
      return nanos;
   }

   @GuardedBy("lock")
   private void reportTimeout(State state) throws TimeoutException {
      List<String> timedOutStates = new ArrayList<String>(1);
      for (String dependencyName : state.dependencies) {
         State dependency = stateMap.get(dependencyName);
         if (!dependency.signalled) {
            timedOutStates.add(dependencyName);
         }
      }
      String errorMessage = String.format("Timed out waiting to enter state %s. Dependencies not satisfied are %s",
            state.name, timedOutStates);
      log.trace(errorMessage);
      throw new TimeoutException(errorMessage);
   }

   /**
    * Equivalent to {@code enter(state); exit(state);}.
    */
   public void advance(String state) throws TimeoutException, InterruptedException {
      enter(state);
      exit(state);
   }

   /**
    * Enter a state and block until all its dependencies have been exited, using the default timeout.
    */
   public void enter(String stateName) throws TimeoutException, InterruptedException {
      doEnter(stateName, defaultTimeoutNanos);
   }

   /**
    * Stop doing anything on {@code enter()} or {@code exit()}.
    * Existing threads waiting in {@code enter()} will be waken up.
    */
   public void stop() {
      lock.lock();
      try {
         log.tracef("Stopping sequencer %s", toString());
         running = false;
         condition.signalAll();
      } finally {
         lock.unlock();
      }
   }

   public String toString() {
      lock.lock();
      try {
         StringBuilder sb = new StringBuilder();
         sb.append("Sequencer{ ");
         for (LogicalThread thread : logicalThreads.values()) {
            sb.append(thread);
            sb.append("; ");
         }
         sb.append("global order: ").append(getOrderString());
         sb.append("}");
         return sb.toString();
      } finally {
         lock.unlock();
      }
   }

   public boolean isInState(String stateName) {
      lock.lock();
      try {
         State state = stateMap.get(stateName);
         LogicalThread logicalThread = logicalThreads.get(state.threadName);
         return stateName.equals(logicalThread.currentState);
      } finally {
         lock.unlock();
      }
   }

   public boolean isAfterState(String stateName) {
      lock.lock();
      try {
         State state = stateMap.get(stateName);
         return state.signalled;
      } finally {
         lock.unlock();
      }
   }

   public boolean isInOrAfterState(String stateName) {
      lock.lock();
      try {
         State state = stateMap.get(stateName);
         if (state.signalled) return true;
         LogicalThread logicalThread = logicalThreads.get(state.threadName);
         return stateName.equals(logicalThread.currentState);
      } finally {
         lock.unlock();
      }
   }

   private interface StatesVisitor {
      void visitStates(List<String> visitedStates);

      void visitCycle(Collection<String> remainingStates);
   }

   private static class State {
      final String threadName;
      final String name;
      final List<String> dependencies;
      Callable<Object> action;
      boolean signalled;

      public State(String threadName, String name) {
         this.threadName = threadName;
         this.name = name;
         this.dependencies = new ArrayList<String>();
      }
   }

   private static class LogicalThread {
      final String name;
      final List<String> states;
      String currentState;

      public LogicalThread(String name, List<String> states) {
         this.name = name;
         this.states = states;
      }

      public void setCurrentState(String state) {
         this.currentState = state;
      }

      public String toString() {
         StringBuilder sb = new StringBuilder();
         sb.append(name).append(": ");
         for (int i = 0; i < states.size(); i++) {
            String state = states.get(i);
            if (i > 0) {
               sb.append(" < ");
            }
            if (state.equals(currentState)) {
               sb.append("*");
            }
            sb.append(state);
         }
         return sb.toString();
      }
   }
}
