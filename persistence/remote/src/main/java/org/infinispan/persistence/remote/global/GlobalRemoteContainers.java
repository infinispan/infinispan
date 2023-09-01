package org.infinispan.persistence.remote.global;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * GlobalRemoteContainer.
 *
 * @author Tristan Tarrant
 * @since 15.0
 */
@Scope(Scopes.GLOBAL)
public interface GlobalRemoteContainers {

   CompletionStage<RemoteCacheManager> cacheContainer(String name, Marshaller marshaller);

}
