package org.infinispan.client.rest;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.api.CacheContainerAdmin;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestCacheClient {

   /**
    * Returns the name of the cache
    */
   String name();

   CompletionStage<RestResponse> keys();

   /**
    * Retrieves the cache configuration
    */
   CompletionStage<RestResponse> configuration();

   /**
    * Clears a cache
    */
   CompletionStage<RestResponse> clear();

   /**
    * Obtains the total number of elements in the cache
    */
   CompletionStage<RestResponse> size();

   /**
    * POSTs a key/value to the cache as text/plain
    *
    * @param key
    * @param value
    * @return
    */
   CompletionStage<RestResponse> post(String key, String value);

   /**
    * POSTs a key/value to the cache as text/plain with the specified expiration
    *
    * @param key
    * @param value
    * @param ttl
    * @param maxIdle
    * @return
    */
   CompletionStage<RestResponse> post(String key, String value, long ttl, long maxIdle);

   /**
    * POSTs a key/value to the cache with the specified encoding
    *
    * @param key
    * @param value
    * @return
    */
   CompletionStage<RestResponse> post(String key, RestEntity value);

   /**
    * POSTs a key/value to the cache with the specified encoding and expiration
    *
    * @param key
    * @param value
    * @param ttl
    * @param maxIdle
    * @return
    */
   CompletionStage<RestResponse> post(String key, RestEntity value, long ttl, long maxIdle);

   /**
    * PUTs a key/value to the cache as text/plain
    *
    * @param key
    * @param value
    * @return
    */
   CompletionStage<RestResponse> put(String key, String value);

   /**
    * PUTs a key/value to the cache as text/plain with the specified expiration
    *
    * @param key
    * @param value
    * @param ttl
    * @param maxIdle
    * @return
    */
   CompletionStage<RestResponse> put(String key, String value, long ttl, long maxIdle);

   /**
    * PUTs a key/value to the cache with the specified encoding
    *
    * @param key
    * @param value
    * @return
    */
   CompletionStage<RestResponse> put(String key, RestEntity value);

   /**
    * PUTs a key/value to the cache with the specified encoding and expiration
    *
    * @param key
    * @param value
    * @param ttl
    * @param maxIdle
    * @return
    */
   CompletionStage<RestResponse> put(String key, RestEntity value, long ttl, long maxIdle);

   /**
    * GETs a key from the cache
    *
    * @param key
    * @return
    */
   CompletionStage<RestResponse> get(String key);

   /**
    * Similar to {@link #get(String)} but only retrieves headers
    * @param key
    * @return
    */
   CompletionStage<RestResponse> head(String key);

   /**
    * DELETEs an entry from the cache
    *
    * @param key
    * @return
    */
   CompletionStage<RestResponse> remove(String key);

   /**
    * Creates the cache using the supplied template name
    *
    * @param template the name of a template
    * @param flags    any flags to apply to the create operation, e.g. {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag#VOLATILE}
    * @return
    */
   CompletionStage<RestResponse> createWithTemplate(String template, CacheContainerAdmin.AdminFlag... flags);

   /**
    * Obtains statistics for the cache
    *
    * @return
    */
   CompletionStage<RestResponse> stats();

   /**
    * Creates the cache using the supplied configuration
    *
    * @param configuration the configuration, either in XML or JSON format
    * @param flags         any flags to apply to the create operation, e.g. {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag#VOLATILE}
    * @return
    */
   CompletionStage<RestResponse> createWithConfiguration(RestEntity configuration, CacheContainerAdmin.AdminFlag... flags);

   /**
    * Removes the cache
    *
    * @return
    */
   CompletionStage<RestResponse> delete();

   /**
    * Executes an Ickle-query
    * @param query the ickle query
    */
   CompletionStage<RestResponse> query(String query);

   /**
    * Executes an Ickle-query
    * @param query the ickle query
    * @param maxResults the maximum number of results to return
    * @param offset the offset within the result from which to return results
    * @param queryMode the query mode
    */
   CompletionStage<RestResponse> query(String query, int maxResults, int offset, RestQueryMode queryMode);

   /**
    * @return the status of all backup sites
    */
   CompletionStage<RestResponse> xsiteBackups();

   /**
    * @return the status of a single backup site
    */
   CompletionStage<RestResponse> backupStatus(String site);

   /**
    *  Take a backup site offline
    */
   CompletionStage<RestResponse> takeSiteOffline(String site);

   /**
    * Bring back a backup site online
    */
   CompletionStage<RestResponse> bringSiteOnline(String site);

   /**
    * Starts the state push to a backup site
    */
   CompletionStage<RestResponse> pushSiteState(String site);

   /**
    * Cancels the state push
    */
   CompletionStage<RestResponse> cancelPushState(String site);

   /**
    * Cancel the receiving state on a backup site
    */
   CompletionStage<RestResponse> cancelReceiveState(String site);

   /**
    * Obtain the status of a state push to a backup site
    */
   CompletionStage<RestResponse> pushStateStatus();

   /**
    * Get the configuration used to automatically take a backup site offline
    */
   CompletionStage<RestResponse> getXSiteTakeOfflineConfig(String site);

   /**
    * Updates the configuration used to automatically take a backup site offline
    */
   CompletionStage<RestResponse> updateXSiteTakeOfflineConfig(String site, int afterFailures, long minTimeToWait);

   /**
    * Clear the status of a state push in a site
    */
   CompletionStage<RestResponse> clearPushStateStatus();

   /**
    * Check if the cache exists
    */
   CompletionStage<RestResponse> exists();
}
