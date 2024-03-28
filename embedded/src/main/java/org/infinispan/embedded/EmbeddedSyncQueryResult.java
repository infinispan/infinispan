package org.infinispan.embedded;

import java.util.OptionalLong;

import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.sync.SyncQueryResult;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.embedded.impl.EmbeddedUtil;

/**
 * @param <R>
 * @since 15.0
 */
public class EmbeddedSyncQueryResult<R> implements SyncQueryResult<R> {
   private final QueryResult<R> result;

   EmbeddedSyncQueryResult(QueryResult<R> result) {
      this.result = result;
   }

   @Override
   public OptionalLong hitCount() {
      return OptionalLong.of(result.count().value());
   }

   @Override
   public CloseableIterable<R> results() {
      return EmbeddedUtil.closeableIterable(result.list());
   }

   @Override
   public void close() {
   }
}
