package org.infinispan.rest.framework;

import java.util.Map;

/**
 * @since 10.0
 */
public interface LookupResult {

   /**
    * Status of the lookup operation.
    */
   enum Status {
      /**
       * A resource was found to handle the request
       */
      FOUND,
      /**
       * A resource was not found
       */
      NOT_FOUND,
      /**
       * A resource was found but the Http Method is not allowed
       */
      INVALID_METHOD,
      /**
       * A resource was found but the action parameter is missing or incorrect
       */
      INVALID_ACTION
   }

   /**
    * Returns the invocation to carry out a {@link RestRequest}
    */
   Invocation getInvocation();

   /**
    * In case the invocation contains paths with {@link org.infinispan.rest.framework.impl.VariablePathItem},
    * returns the value for each variable or empty otherwise.
    */
   Map<String, String> getVariables();

   /**
    * @return Status of the lookup operation
    */
   Status getStatus();
}
