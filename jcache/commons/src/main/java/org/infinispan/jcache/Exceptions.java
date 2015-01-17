package org.infinispan.jcache;

import javax.cache.CacheException;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.EntryProcessorException;

import org.infinispan.commons.CacheListenerException;

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

   //TODO: was package-level initially
   public static RuntimeException launderCacheLoaderException(Exception e) {
      if (!(e instanceof CacheLoaderException))
         return new CacheLoaderException("Exception in CacheLoader", e);
      else 
         return (CacheLoaderException) e;
   }

   //TODO: was package-level initially
   public static RuntimeException launderCacheWriterException(Exception e) {
      if (!(e instanceof CacheWriterException))
         return new CacheWriterException("Exception in CacheWriter", e);

      return new CacheException("Error in CacheWriter", e);
   }

   //TODO: was package-level initially
   public static RuntimeException launderEntryProcessorException(Exception e) {
      if (e instanceof CacheException)
         return (CacheException) e;

      return new EntryProcessorException(e);
   }

   //TODO: was package-level initially
   public static RuntimeException launderCacheListenerException(CacheListenerException e) {
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
