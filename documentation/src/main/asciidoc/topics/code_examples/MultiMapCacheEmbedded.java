import org.infinispan.configuration.cache.ConfigurationBuilder;

// Create or obtain your EmbeddedCacheManager
EmbeddedCacheManager cm = ... ;
// Configure the multimap cache
ConfigurationBuilder cb = ...;

// create or obtain a MultimapCacheManager passing the EmbeddedCacheManager
MultimapCacheManager multimapCacheManager = EmbeddedMultimapCacheManagerFactory.from(cm);

// define the configuration for the multimap cache
multimapCacheManager.defineConfiguration(multimapCacheName, cb.build());

// get the multimap cache
multimapCache = multimapCacheManager.get(multimapCacheName);
