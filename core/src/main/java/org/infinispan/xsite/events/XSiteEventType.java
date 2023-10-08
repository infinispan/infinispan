package org.infinispan.xsite.events;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Types of {@link XSiteEvent}.
 *
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_EVENT_TYPE)
public enum XSiteEventType {
   /**
    * Sent from site A to site B, notifies site B that a new connection to A is available.
    */
   @ProtoEnumValue(1)
   SITE_CONNECTED,
   /**
    * Sent from site A to site B, notifies site B that it can send state (automatic cross-site state transfer must be
    * enabled).
    */
   @ProtoEnumValue(2)
   STATE_REQUEST,
   /**
    * When a cache 'c' starts in site A and has site B as asynchronous backup, this event is sent from site A to site B
    * to notify site B to send state (automatic cross-site state transfer must be enabled).
    */
   @ProtoEnumValue(3)
   INITIAL_STATE_REQUEST,
}
