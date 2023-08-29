package org.infinispan.persistence.remote.global;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * GlobalRemoteContainer.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@Scope(Scopes.GLOBAL)
public interface GlobalRemoteContainers {

   CompletionStage<RemoteCacheManager> cacheContainer(String name);

}
