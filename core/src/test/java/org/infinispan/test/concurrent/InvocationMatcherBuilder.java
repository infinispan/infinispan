package org.infinispan.test.concurrent;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

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

   public InvocationMatcherBuilder(String methodName) {
      this.methodName = methodName;
   }

   public InvocationMatcher build() {
      Matcher[] matchersArray = argumentMatchers != null ?
            argumentMatchers.toArray(new Matcher[argumentMatchers.size()]) : null;
      return new DefaultInvocationMatcher(methodName, instanceMatcher, matchersArray);
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

   public InvocationMatcherBuilder withThis(Matcher<Object> matcher) {
      instanceMatcher = matcher;
      return this;
   }
}
