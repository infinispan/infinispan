package org.infinispan.search.mapper.work;

import java.util.concurrent.CompletableFuture;

/**
 * @author Fabio Massimo Ercoli
 */
public interface SearchIndexer extends AutoCloseable {

   CompletableFuture<?> add(Object providedId, String routingKey, Object entity);

   CompletableFuture<?> addOrUpdate(Object providedId, String routingKey, Object entity);

   CompletableFuture<?> delete(Object providedId, String routingKey, Object entity);

   CompletableFuture<?> purge(Object providedId, String routingKey);

   @Override
   void close();

}
