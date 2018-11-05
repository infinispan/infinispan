package org.infinispan.rest.framework.impl;

import java.util.Map;

import org.infinispan.rest.framework.Invocation;
import org.infinispan.rest.framework.LookupResult;

/**
 * @since 10.0
 */
public class LookupResultImpl implements LookupResult {

   private final Invocation invocation;
   private final Map<String, String> variables;

   LookupResultImpl(Invocation invocation, Map<String, String> variables) {
      this.invocation = invocation;
      this.variables = variables;
   }

   public Invocation getInvocation() {
      return invocation;
   }

   public Map<String, String> getVariables() {
      return variables;
   }
}
