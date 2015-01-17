package org.infinispan.jcache;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.EntryProcessorResult;

/**
 * Successful entry processor result wrapper.
 *
 * @author Galder Zamarreño
 * @since 7.0
 */
public class SuccessEntryProcessorResult<T> implements EntryProcessorResult<T> {

   private final T result;

   public SuccessEntryProcessorResult(T result) {
      this.result = result;
   }

   @Override
   public T get() throws EntryProcessorException {
      return result;
   }

}
