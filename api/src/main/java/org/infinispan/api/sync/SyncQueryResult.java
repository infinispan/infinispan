package org.infinispan.api.sync;

import java.util.OptionalLong;

import org.infinispan.api.common.CloseableIterable;

/**
 * @since 14.0
 **/
public interface SyncQueryResult<R> extends CloseableIterable<R> {
   OptionalLong hitCount();

   void close();
}
