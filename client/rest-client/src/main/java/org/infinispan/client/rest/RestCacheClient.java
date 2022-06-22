package org.infinispan.client.rest;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.XSiteStateTransferMode;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestCacheClient {

   /**
    * Returns the name of the cache
    */
   String name();

   /**
    * Retrieve all keys
    *
    * @return Response with inputStream to get all the keys
    */
   CompletionStage<RestResponse> keys();

   /**
    * Retrieve keys limited by count
    *
    * @param limit The maximum number of keys to retrieve
    * @return Response with InputStream to get the keys
    */
   CompletionStage<RestResponse> keys(int limit);

   /**
    * Retrieves entries without metadata
    *
    * @return Response with InputStream to get the entries
    */
   default CompletionStage<RestResponse> entries() {
      return entries(false);
   }

   /**
    * Retrieves entries without metadata
    *
    * @param contentNegotiation if true, the server will convert keys and values to a readable format and return headers with the negotiated media type.
    * @return Response with InputStream to get the entries
    */
   CompletionStage<RestResponse> entries(boolean contentNegotiation);

   /**
    * Retrieves entries limited by count
    *
    * @param limit: The maximum number of entries to retrieve, or -1 to retrieve all
    */
   CompletionStage<RestResponse> entries(int limit);

   /**
    * Retrieves entries with limit and metadata
    *
    * @param limit:    The maximum number of entries to retrieve, or -1 to retrieve all
    * @param metadata: if true, includes the metadata for each entry
    */
   CompletionStage<RestResponse> entries(int limit, boolean metadata);

   /**
    * Retrieves all keys from the cache with a specific MediaType or list of MediaTypes.
    */
   CompletionStage<RestResponse> keys(String mediaType);

   /**
    * Retrieves the cache configuration
    */
   default CompletionStage<RestResponse> configuration() {
      return configuration(null);
   }

   /**
    * Retrieves the cache configuration with a specific MediaType or list of MediaTypes.
    */
   CompletionStage<RestResponse> configuration(String mediaType);

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
    * PUT a key/value to the cache with custom media types for keys and values
    */
   CompletionStage<RestResponse> put(String key, String keyContentType, RestEntity value);

   /**
    * Same as {@link #put(String, String, RestEntity)} but allowing custom headers.
    */
   CompletionStage<RestResponse> put(String key, String keyContentType, RestEntity value, Map<String, String> headers);

   /**
    * PUT an entry with metadata.
    *
    * @param key The key
    * @param keyContentType The {@link MediaType} of the key
    * @param value a {@link RestEntity} containing the value and its MediaType
    * @param ttl The time to live value
    * @param maxIdle The max idle value
    * @return
    */
   CompletionStage<RestResponse> put(String key, String keyContentType, RestEntity value, long ttl, long maxIdle);

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
    * Same as {@link #put(String, RestEntity)} also allowing one or more {@link org.infinispan.context.Flag} to be passed.
    */
   CompletionStage<RestResponse> put(String key, RestEntity value, String... flags);

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
    * Same as {@link #get(String)} but allowing custom headers.
    */
   CompletionStage<RestResponse> get(String key, Map<String, String> headers);

   /**
    * GETs a key from the cache with a specific MediaType or list of MediaTypes.
    */
   CompletionStage<RestResponse> get(String key, String mediaType);

   /**
    * Same as {@link #get(String, String)} but with an option to return extended headers.
    */
   CompletionStage<RestResponse> get(String key, String mediaType, boolean extended);


   /**
    * Similar to {@link #get(String)} but only retrieves headers
    *
    * @param key
    * @return
    */
   CompletionStage<RestResponse> head(String key);

   /**
    * Similar to {@link #head(String)} but allowing custom headers
    */
   CompletionStage<RestResponse> head(String key, Map<String, String> headers);

   /**
    * DELETEs an entry from the cache
    *
    * @param key
    * @return
    */
   CompletionStage<RestResponse> remove(String key);

   /**
    * Same as {@link #remove(String)} but allowing custom headers
    */
   CompletionStage<RestResponse> remove(String test, Map<String, String> headers);

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
    * Obtain metrics about the distribution of data of the cache.
    *
    * @return
    */
   CompletionStage<RestResponse> distribution();

   /**
    * Return the nodes which the key is present for a specific cache.
    *
    * @param key: the key to search.
    */
   CompletionStage<RestResponse> distribution(String key);

   /**
    * Creates the cache using the supplied configuration
    *
    * @param configuration the configuration, in XML, JSON or YAML format
    * @param flags         any flags to apply to the create operation, e.g. {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag#VOLATILE}
    * @return
    */
   CompletionStage<RestResponse> createWithConfiguration(RestEntity configuration, CacheContainerAdmin.AdminFlag... flags);

   /**
    * Updates the cache configuration
    *
    * @param configuration the configuration, in XML, JSON or YAML format
    * @param flags         any flags to apply to the update operation, e.g. {@link org.infinispan.commons.api.CacheContainerAdmin.AdminFlag#VOLATILE}
    * @return
    */
   CompletionStage<RestResponse> updateWithConfiguration(RestEntity configuration, CacheContainerAdmin.AdminFlag... flags);

   /**
    * Removes the cache
    *
    * @return
    */
   CompletionStage<RestResponse> delete();

   /**
    * Executes an Ickle-query
    *
    * @param query the ickle query
    */
   default CompletionStage<RestResponse> query(String query) {
      return query(query, false);
   }

   /**
    * Executes an Ickle-query
    *
    * @param query the ickle query
    * @param local if true, query is restricted to the data present in the node that process the request.
    */
   CompletionStage<RestResponse> query(String query, boolean local);

   /**
    * Executes an Ickle-query
    *
    * @param query      the ickle query
    * @param maxResults the maximum number of results to return
    * @param offset     the offset within the result from which to return results
    */
   CompletionStage<RestResponse> query(String query, int maxResults, int offset);

   /**
    * @return the status of all backup sites
    */
   CompletionStage<RestResponse> xsiteBackups();

   /**
    * @return the status of a single backup site
    */
   CompletionStage<RestResponse> backupStatus(String site);

   /**
    * Take a backup site offline
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
    * Returns the cross-site replication state transfer mode.
    *
    * @see XSiteStateTransferMode
    */
   CompletionStage<RestResponse> xSiteStateTransferMode(String site);

   /**
    * Sets the cross-site replication state transfer mode.
    *
    * @see XSiteStateTransferMode
    */
   CompletionStage<RestResponse> xSiteStateTransferMode(String site, XSiteStateTransferMode mode);

   /**
    * Check if the cache exists
    */
   CompletionStage<RestResponse> exists();

   /**
    * Execute a Rolling Upgrade processing
    */
   CompletionStage<RestResponse> synchronizeData(Integer readBatch, Integer threads);

   /**
    * Execute a Rolling Upgrade processing using defaults.
    */
   CompletionStage<RestResponse> synchronizeData();

   /**
    * Disconnects the target cluster from the source cluster after a Rolling Upgrade
    */
   CompletionStage<RestResponse> disconnectSource();

   /**
    * Connects the target cluster to a source cluster before a Rolling Upgrade
    *
    * @param remoteStoreJsonConfig The remote-store config as JSON
    */
   CompletionStage<RestResponse> connectSource(RestEntity remoteStoreJsonConfig);

   /**
    * Checks if the cache is connected through a remote store to perform rolling upgrades
    */
   CompletionStage<RestResponse> sourceConnected();

   /**
    * Return the remote store configuration in case the case has been connected to another cluster using
    * {@link #connectSource(RestEntity)}
    */
   CompletionStage<RestResponse> sourceConnection();

   /**
    * Rebuild the search indexes of the cache based on its data.
    */
   CompletionStage<RestResponse> reindex();

   /**
    * Same as {@link #reindex()} but only considers data from the local cluster member.
    */
   CompletionStage<RestResponse> reindexLocal();

   /**
    * Deletes all the indexes from the cache.
    */
   CompletionStage<RestResponse> clearIndex();

   /**
    * Update index schema for the current cache.
    */
   CompletionStage<RestResponse> updateIndexSchema();

   /**
    * Obtain statistics about queries.
    *
    * @deprecated Use {@link #searchStats()} instead.
    */
   @Deprecated
   CompletionStage<RestResponse> queryStats();

   /**
    * Obtain statistics about the indexes.
    *
    * @deprecated Use {@link #searchStats()} instead.
    */
   @Deprecated
   CompletionStage<RestResponse> indexStats();

   /**
    * Clear runtime query statistics.
    *
    * @deprecated Use {@link #searchStats()} and {@link #clearSearchStats()}.
    */
   @Deprecated
   CompletionStage<RestResponse> clearQueryStats();


   /**
    * Obtains details about the cache
    */
   CompletionStage<RestResponse> details();


   /**
    * Obtain query and indexing statistics for the cache.
    */
   CompletionStage<RestResponse> searchStats();

   /**
    * Clear search stats.
    */
   CompletionStage<RestResponse> clearSearchStats();

   /**
    * Enables automatic rebalancing for the cache.
    */
   CompletionStage<RestResponse> enableRebalancing();

   /**
    * Disables automatic rebalancing for the cache.
    */
   CompletionStage<RestResponse> disableRebalancing();

   /**
    * Updates a configuration attribute.
    */
   CompletionStage<RestResponse> updateConfigurationAttribute(String attribute, String value);

   /**
    * Retrieves all available configuration attributes for this cache
    */
   CompletionStage<RestResponse> configurationAttributes();

   /**
    * Retrieves all available configuration attributes for this cache optionally including values and types
    */
   CompletionStage<RestResponse> configurationAttributes(boolean full);

   /**
    * Retrieves the Cache's Availability status.
    */
   CompletionStage<RestResponse> getAvailability();

   /**
    * Sets the Cache's Avaialability
    */
   CompletionStage<RestResponse> setAvailability(String availability);
}
