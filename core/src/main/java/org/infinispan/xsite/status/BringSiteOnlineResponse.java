package org.infinispan.xsite.status;

/**
 * The return value of {@link TakeOfflineManager#bringSiteOnline(String)}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public enum BringSiteOnlineResponse {
   NO_SUCH_SITE,
   ALREADY_ONLINE,
   BROUGHT_ONLINE
}
