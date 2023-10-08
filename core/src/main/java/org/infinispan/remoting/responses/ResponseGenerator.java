package org.infinispan.remoting.responses;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * A component that generates responses as is expected by different cache setups
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface ResponseGenerator {
   ResponseGenerator INSTANCE = new DefaultResponseGenerator();

   Response getResponse(CacheRpcCommand command, Object returnValue);
}
