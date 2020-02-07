package org.infinispan.xsite.status;

/**
 * The return value of {@link TakeOfflineManager#takeSiteOffline(String)}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public enum TakeSiteOfflineResponse {
   NO_SUCH_SITE,
   ALREADY_OFFLINE,
   TAKEN_OFFLINE
}
