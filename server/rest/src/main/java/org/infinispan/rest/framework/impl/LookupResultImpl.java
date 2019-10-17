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
   private final Status status;

   static LookupResult NOT_FOUND = new LookupResultImpl(Status.NOT_FOUND);
   static LookupResult INVALID_METHOD = new LookupResultImpl(Status.INVALID_METHOD);
   static LookupResult INVALID_ACTION = new LookupResultImpl(Status.INVALID_ACTION);

   LookupResultImpl(Invocation invocation, Map<String, String> variables, Status status) {
      this.invocation = invocation;
      this.variables = variables;
      this.status = status;
   }

   private LookupResultImpl(Status status) {
      this.invocation = null;
      this.variables = null;
      this.status = status;
   }

   public Invocation getInvocation() {
      return invocation;
   }

   public Map<String, String> getVariables() {
      return variables;
   }

   @Override
   public Status getStatus() {
      return status;
   }
}
