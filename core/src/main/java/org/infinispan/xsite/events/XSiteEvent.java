package org.infinispan.xsite.events;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.ByteString;

/**
 * Represents an event related to cross-site.
 * <p>
 * This event may be local to the cluster, for example to notify a sites view change, or going remotely to other sites.
 *
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_EVENT)
public final class XSiteEvent {

   @ProtoField(1)
   final XSiteEventType type;
   @ProtoField(2)
   final ByteString siteName;
   @ProtoField(3)
   final ByteString cacheName;

   @ProtoFactory
   XSiteEvent(XSiteEventType type, ByteString siteName, ByteString cacheName) {
      this.type = Objects.requireNonNull(type);
      this.siteName = siteName;
      this.cacheName = cacheName;
   }

   public static XSiteEvent createConnectEvent(ByteString localSite) {
      return new XSiteEvent(XSiteEventType.SITE_CONNECTED, Objects.requireNonNull(localSite), null);
   }

   public static XSiteEvent createRequestState(ByteString localSite, ByteString cacheName) {
      return new XSiteEvent(XSiteEventType.STATE_REQUEST, Objects.requireNonNull(localSite), Objects.requireNonNull(cacheName));
   }

   public static XSiteEvent createInitialStateRequest(ByteString localSite, ByteString cacheName) {
      return new XSiteEvent(XSiteEventType.INITIAL_STATE_REQUEST, Objects.requireNonNull(localSite), Objects.requireNonNull(cacheName));
   }

   public XSiteEventType getType() {
      return type;
   }

   public ByteString getSiteName() {
      return siteName;
   }

   public ByteString getCacheName() {
      return cacheName;
   }

   @Override
   public String toString() {
      return "XSiteEvent{" +
            "type=" + type +
            ", siteName=" + siteName +
            ", cacheName=" + cacheName +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      XSiteEvent that = (XSiteEvent) o;

      if (type != that.type) return false;
      if (!Objects.equals(siteName, that.siteName)) return false;
      return Objects.equals(cacheName, that.cacheName);
   }

   @Override
   public int hashCode() {
      int result = type.hashCode();
      result = 31 * result + (siteName != null ? siteName.hashCode() : 0);
      result = 31 * result + (cacheName != null ? cacheName.hashCode() : 0);
      return result;
   }
}
