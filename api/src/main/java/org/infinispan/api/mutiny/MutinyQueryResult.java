package org.infinispan.api.mutiny;

import java.util.OptionalLong;

import io.smallrye.mutiny.Multi;

/**
 * @since 14.0
 **/
public interface MutinyQueryResult<R> extends AutoCloseable {
   OptionalLong hitCount();

   Multi<R> results();

   void close();
}
