package org.infinispan.spring.session;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.spring.provider.SpringCache;
import org.springframework.session.FindByIndexNameSessionRepository;
import org.springframework.session.MapSession;

/**
 * Session Repository for Infinispan in Embedded mode.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 * @see FindByIndexNameSessionRepository
 */
public class InfinispanEmbeddedSessionRepository extends AbstractInfinispanSessionRepository implements FindByIndexNameSessionRepository<MapSession> {

   protected final PrincipalNameResolver principalNameResolver = new PrincipalNameResolver();

   /**
    * Creates new repository based on {@link SpringCache}
    *
    * @param cache Cache which shall be used for session repository.
    */
   public InfinispanEmbeddedSessionRepository(SpringCache cache) {
      super(cache, new EmbeddedApplicationPublishedBridge(cache));
   }

   @Override
   public Map<String, MapSession> findByIndexNameAndIndexValue(String indexName, String indexValue) {
      if (!PRINCIPAL_NAME_INDEX_NAME.equals(indexName)) {
         return Collections.emptyMap();
      }

      return cache.getNativeCache().values().stream()
            .map(cacheValue -> (MapSession) cacheValue)
            .filter(session -> indexValue.equals(principalNameResolver.resolvePrincipal(session)))
            .collect(Collectors.toMap(MapSession::getId, Function.identity()));
   }
}
