package org.infinispan.jcache;

import org.infinispan.commons.CacheListenerException;
import org.infinispan.persistence.spi.PersistenceException;

import javax.cache.CacheException;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessorException;

/**
 * Exception laundering utility class.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public class Exceptions {

   // Suppresses default constructor, ensuring non-instantiability.
   private Exceptions(){
   }

   static RuntimeException launderCacheLoaderException(Exception e) {
      if (!(e instanceof CacheLoaderException)) {
         return new CacheLoaderException("Exception in CacheLoader", e);
      } else {
         return new PersistenceException(e);
      }
   }

   static RuntimeException launderCacheWriterException(Exception e) {
      if (!(e instanceof CacheWriterException)) {
         return new CacheWriterException("Exception in CacheWriter", e);
      } else {
         return new CacheException("Error in CacheWriter", e);
      }
   }

   static RuntimeException launderEntryProcessorException(Exception e) {
      if (!(e instanceof EntryProcessorException)) {
         return new EntryProcessorException(e);
      } else {
         return new CacheException(e);
      }
   }

   static RuntimeException launderCacheListenerException(CacheListenerException e) {
      Throwable cause = e.getCause();

      if (cause instanceof CacheEntryListenerException)
         return (CacheEntryListenerException) cause;

      if (cause instanceof Exception)
         return new CacheEntryListenerException(cause);

      if (cause instanceof Error)
         throw (Error) cause;

      return e;
   }

}
