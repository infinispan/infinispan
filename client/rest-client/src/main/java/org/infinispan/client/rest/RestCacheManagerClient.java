package org.infinispan.client.rest;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.dataconversion.MediaType;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestCacheManagerClient {
   String name();

   default CompletionStage<RestResponse> globalConfiguration() {
      return globalConfiguration(MediaType.APPLICATION_JSON_TYPE);
   }

   CompletionStage<RestResponse> globalConfiguration(String mediaType);

   CompletionStage<RestResponse> cacheConfigurations();

   CompletionStage<RestResponse> cacheConfigurations(String mediaType);

   CompletionStage<RestResponse> info();

   CompletionStage<RestResponse> health(boolean skipBody);

   default CompletionStage<RestResponse> health() {
      return health(false);
   }

   CompletionStage<RestResponse> templates(String mediaType);

   CompletionStage<RestResponse> healthStatus();

   CompletionStage<RestResponse> stats();

   CompletionStage<RestResponse> backupStatuses();

   CompletionStage<RestResponse> bringBackupOnline(String backup);

   CompletionStage<RestResponse> takeOffline(String backup);

   CompletionStage<RestResponse> pushSiteState(String backup);

   CompletionStage<RestResponse> cancelPushState(String backup);

   CompletionStage<RestResponse> caches();
}
