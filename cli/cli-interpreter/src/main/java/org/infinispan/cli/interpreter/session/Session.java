package org.infinispan.cli.interpreter.session;

import java.util.Collection;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.codec.Codec;
import org.infinispan.cli.interpreter.codec.CodecException;
import org.infinispan.manager.EmbeddedCacheManager;

public interface Session {

   /**
    * Obtains the CacheManager to which this session is attached
    */
   EmbeddedCacheManager getCacheManager();

   /**
    * Obtains the currently selected cache. If none has been selected, the default cache is returned
    */
   <K, V> Cache<K, V> getCurrentCache();

   /**
    * Returns the name of the currently selected cache. If none has been selected, the default cache is returned
    */
   String getCurrentCacheName();

   /**
    * Returns a named cache. If the cacheName parameter is null, the current cache is returned
    *
    * @param cacheName
    * @return the cache identified by cacheName
    */
   <K, V> Cache<K, V> getCache(String cacheName);

   /**
    * Sets the current cache.
    *
    * @param cacheName
    */
   void setCurrentCache(String cacheName);

   /**
    * Creates a new cache
    *
    * @param cacheName the name of the new cache
    * @param baseCacheName the existing named cache to use for defaults
    */
   void createCache(String cacheName, String baseCacheName);

   /**
    * Resets the session, by aborting any dangling batches and transactions and updating the timestamp
    */
   void reset();

   /**
    * Returns the time the session was last accessed
    */
   long getTimestamp();

   /**
    * Returns the unique id of this session
    */
   String getId();


   /**
    * Sets the codec to use for encoding/decoding keys/values
    *
    * @param encoding the name of the codec to use
    */
   void setCodec(String encoding) throws CodecException;

   /**
    * Returns the list of available key/value codecs
    */
   Collection<Codec> getCodecs();

   /**
    * Returns the currently active codec used to encode/decode keys/values
    */
   Codec getCodec();

   /**
    * Returns a specific codec
    * @throws CodecException
    */
   Codec getCodec(String parameter) throws CodecException;
}
