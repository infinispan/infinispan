package org.infinispan.rest.framework;

import java.util.Map;

/**
 * @since 10.0
 */
public interface LookupResult {

   /**
    * Returns the invocation to carry out a {@link RestRequest}
    */
   Invocation getInvocation();

   /**
    * In case the invocation contains paths with {@link org.infinispan.rest.framework.impl.VariablePathItem},
    * returns the value for each variable or empty otherwise.
    */
   Map<String, String> getVariables();
}
