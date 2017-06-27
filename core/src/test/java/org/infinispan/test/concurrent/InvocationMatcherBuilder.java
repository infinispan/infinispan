package org.infinispan.test.concurrent;

import java.util.ArrayList;
import java.util.List;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;

/**
 * Creates {@link InvocationMatcher}s.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public class InvocationMatcherBuilder {
   private final String methodName;
   private Matcher instanceMatcher;
   private List<Matcher> argumentMatchers;
   private int matchCount = -1;
   private String inOrAfterState, afterState;
   private StateSequencer stateSequencer;

   public InvocationMatcherBuilder(String methodName) {
      this.methodName = methodName;
   }

   public InvocationMatcher build() {
      Matcher[] matchersArray = argumentMatchers != null ?
            argumentMatchers.toArray(new Matcher[argumentMatchers.size()]) : null;
      InvocationMatcher matcher = new DefaultInvocationMatcher(methodName, instanceMatcher, matchCount, matchersArray);
      if (inOrAfterState != null) {
         matcher = new StateInvocationMatcher(matcher, stateSequencer, StateInvocationMatcher.Relation.IN_OR_AFTER, inOrAfterState);
      }
      if (afterState != null) {
         matcher = new StateInvocationMatcher(matcher, stateSequencer, StateInvocationMatcher.Relation.AFTER, afterState);
      }
      return matcher;
   }

   public InvocationMatcherBuilder withParam(int index, Object expected) {
      Matcher<Object> matcher = CoreMatchers.equalTo(expected);
      return withMatcher(index, matcher);
   }

   public InvocationMatcherBuilder withMatcher(int index, Matcher<?> matcher) {
      if (argumentMatchers == null) {
         argumentMatchers = new ArrayList<Matcher>(index + 1);
      }
      while (argumentMatchers.size() <= index) {
         argumentMatchers.add(null);
      }
      argumentMatchers.set(index, matcher);
      return this;
   }

   public InvocationMatcherBuilder matchCount(int matchCount) {
      this.matchCount = matchCount;
      return this;
   }

   public InvocationMatcherBuilder withThis(Matcher<Object> matcher) {
      instanceMatcher = matcher;
      return this;
   }

   public InvocationMatcherBuilder inOrAfterState(StateSequencer stateSequencer, String stateName) {
      assert stateSequencer != null && (this.stateSequencer == null || this.stateSequencer == stateSequencer);
      this.stateSequencer = stateSequencer;
      this.inOrAfterState = stateName;
      return this;
   }

   public InvocationMatcherBuilder afterState(StateSequencer stateSequencer, String stateName) {
      assert stateSequencer != null && (this.stateSequencer == null || this.stateSequencer == stateSequencer);
      this.stateSequencer = stateSequencer;
      this.afterState = stateName;
      return this;
   }
}
