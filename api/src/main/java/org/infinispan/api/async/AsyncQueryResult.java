package org.infinispan.api.async;

import java.util.OptionalLong;
import java.util.concurrent.Flow;

/**
 * @since 14.0
 **/
public interface AsyncQueryResult<R> extends AutoCloseable {
   OptionalLong hitCount();

   Flow.Publisher<R> results();

   void close();
}
