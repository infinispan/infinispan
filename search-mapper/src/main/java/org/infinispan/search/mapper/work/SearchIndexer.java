package org.infinispan.search.mapper.work;

import java.util.concurrent.CompletableFuture;

/**
 * @author Fabio Massimo Ercoli
 */
public interface SearchIndexer {

   CompletableFuture<?> add(Object providedId, Object entity);

   CompletableFuture<?> addOrUpdate(Object providedId, Object entity);

   CompletableFuture<?> delete(Object providedId, Object entity);

   CompletableFuture<?> purge(Object providedId, String providedRoutingKey);

}
