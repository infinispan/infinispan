package org.infinispan.rest.framework;

import org.infinispan.rest.framework.impl.Invocations;

/**
 * Handles all the logic related to a REST resource.
 *
 * @since 10.0
 */
public interface ResourceHandler {

   /**
    * Return the {@link Invocations} handled by this ResourceHandler.
    */
   Invocations getInvocations();

}
