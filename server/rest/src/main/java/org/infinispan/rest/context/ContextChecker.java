package org.infinispan.rest.context;

import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.InfinispanRequest;

/**
 * Checks if context passed by {@link InfinispanRequest} is correct.
 */
public class ContextChecker {

   private final RestServerConfiguration restServerConfiguration;

   /**
    * Creates new instance of {@link ContextChecker}.
    *
    * @param restServerConfiguration Rest Server Configuration.
    */
   public ContextChecker(RestServerConfiguration restServerConfiguration) {
      this.restServerConfiguration = restServerConfiguration;
   }

   /**
    * Throws {@link WrongContextException} if context is incorrect.
    *
    * @param request {@link InfinispanRequest} object.
    * @throws WrongContextException Thrown if context is incorrect.
    */
   public void checkContext(InfinispanRequest request) throws WrongContextException {
      if (restServerConfiguration.startTransport() == true && !request.getContext().equals(restServerConfiguration.contextPath())) {
         throw new WrongContextException();
      }
   }
}
