package org.infinispan.xsite.events;

/**
 * Types of {@link XSiteEvent}.
 *
 * @since 15.0
 */
public enum XSiteEventType {
   /**
    * Sent from site A to site B, notifies site B that a new connection to A is available.
    */
   SITE_CONNECTED,
   /**
    * Sent from site A to site B, notifies site B that it can send state (automatic cross-site state transfer must be enabled).
    */
   STATE_REQUEST
}
