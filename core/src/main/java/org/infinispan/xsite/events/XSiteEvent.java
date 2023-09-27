package org.infinispan.xsite.events;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;

import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.util.ByteString;

/**
 * Represents an event related to cross-site.
 * <p>
 * This event may be local to the cluster, for example to notify a sites view change, or going remotely to other sites.
 *
 * @since 15.0
 */
public final class XSiteEvent {

   private static final XSiteEventType[] CACHED_TYPE = XSiteEventType.values();

   private final XSiteEventType type;
   private final ByteString siteName;
   private final ByteString cacheName;

   private XSiteEvent(XSiteEventType type, ByteString siteName, ByteString cacheName) {
      this.type = Objects.requireNonNull(type);
      this.siteName = siteName;
      this.cacheName = cacheName;
   }

   private static XSiteEventType typeFrom(int ordinal) {
      return CACHED_TYPE[ordinal];
   }

   public static XSiteEvent createConnectEvent(ByteString localSite) {
      return new XSiteEvent(XSiteEventType.SITE_CONNECTED, Objects.requireNonNull(localSite), null);
   }

   public static XSiteEvent createRequestState(ByteString localSize, ByteString cacheName) {
      return new XSiteEvent(XSiteEventType.STATE_REQUEST, Objects.requireNonNull(localSize), Objects.requireNonNull(cacheName));
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

   public static void writeTo(ObjectOutput output, XSiteEvent event) throws IOException {
      MarshallUtil.marshallEnum(event.type, output);
      switch (event.type) {
         case SITE_CONNECTED:
            ByteString.writeObject(output, event.siteName);
            return;
         case STATE_REQUEST:
            ByteString.writeObject(output, event.siteName);
            ByteString.writeObject(output, event.cacheName);
      }
   }

   public static XSiteEvent readFrom(ObjectInput input) throws IOException {
      var type = MarshallUtil.unmarshallEnum(input, XSiteEvent::typeFrom);
      assert type != null;
      switch (type) {
         case SITE_CONNECTED:
            return createConnectEvent(ByteString.readObject(input));
         case STATE_REQUEST:
            return createRequestState(ByteString.readObject(input), ByteString.readObject(input));
         default:
            throw new IllegalStateException();
      }
   }
}
