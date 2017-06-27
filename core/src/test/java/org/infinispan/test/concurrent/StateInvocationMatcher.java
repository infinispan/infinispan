package org.infinispan.test.concurrent;

public class StateInvocationMatcher implements InvocationMatcher {
   private final InvocationMatcher matcher;
   private final StateSequencer stateSequencer;
   private final String stateName;
   private final Relation relation;

   public StateInvocationMatcher(InvocationMatcher matcher, StateSequencer stateSequencer, Relation relation, String stateName) {
      this.matcher = matcher;
      this.stateSequencer = stateSequencer;
      this.relation = relation;
      this.stateName = stateName;
   }

   @Override
   public boolean accept(Object instance, String methodName, Object[] arguments) {
      boolean accept = false;
      switch (relation) {
         case IN:
            accept = stateSequencer.isInState(stateName);
            break;
         case IN_OR_AFTER:
            accept = stateSequencer.isInOrAfterState(stateName);
            break;
         case AFTER:
            accept = stateSequencer.isAfterState(stateName);
            break;
         default:
            throw new IllegalStateException(String.valueOf(relation));
      }
      if (accept) {
         return matcher.accept(instance, methodName, arguments);
      }
      return false;
   }

   public enum Relation {
      IN,
      IN_OR_AFTER,
      AFTER
   }
}
