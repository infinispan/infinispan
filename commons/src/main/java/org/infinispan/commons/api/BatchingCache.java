package org.infinispan.commons.api;

/**
 * The BatchingCache is implemented by all caches which support batching
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
public interface BatchingCache {
   /**
    * Starts a batch.  All operations on the current client thread are performed as a part of this batch, with locks
    * held for the duration of the batch and any remote calls delayed till the end of the batch.
    * <p/>
    *
    * @return true if a batch was successfully started; false if one was available and already running.
    */
   boolean startBatch();

   /**
    * Completes a batch if one has been started using {@link #startBatch()}.  If no batch has been started, this is a
    * no-op.
    * <p/>
    *
    * @param successful if true, the batch completes, otherwise the batch is aborted and changes are not committed.
    */
   void endBatch(boolean successful);
}
