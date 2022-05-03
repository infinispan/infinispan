package org.infinispan.hotrod.impl.operations;

import java.util.concurrent.CompletionStage;

public interface RetryAwareCompletionStage<E> extends CompletionStage<E> {
   /**
    * Returns whether this operation had to be retried on another server than the first one picked.
    *
    * @return {@code true} if the operation had to be retried on another server, {@code false} if it completed without
    * retry or {@code null} if the operation is not yet complete.
    */
   Boolean wasRetried();
}
