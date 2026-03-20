package org.infinispan.rest.framework.impl;

import org.infinispan.rest.RestServer;
import org.infinispan.rest.framework.ResourceManager;
import org.infinispan.rest.framework.RestDispatcher;
import org.infinispan.security.impl.Authorizer;

public class ValidatingRestServer extends RestServer {

   @Override
   protected RestDispatcher createDispatcher(ResourceManager manager, Authorizer authorizer) {
      return new ValidatingRestDispatcher(super.createDispatcher(manager, authorizer));
   }
}
