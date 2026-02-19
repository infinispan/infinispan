package org.infinispan.server.resp.filter;

import java.nio.charset.StandardCharsets;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.commons.util.GlobMatcher;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * A {@link CacheEventFilter} that matches cache entry keys against a glob pattern.
 *
 * @since 16.0
 */
@ProtoTypeId(ProtoStreamTypeIds.RESP_EVENT_LISTENER_GLOB_FILTER)
public class EventListenerGlobFilter implements CacheEventFilter<Object, Object> {

   @ProtoField(number = 1)
   final String glob;

   private final transient byte[] pattern;

   @ProtoFactory
   public EventListenerGlobFilter(String glob) {
      this.glob = glob;
      this.pattern = glob.getBytes(StandardCharsets.US_ASCII);
   }

   @Override
   public boolean accept(Object key, Object oldValue, Metadata oldMetadata, Object newValue, Metadata newMetadata, EventType eventType) {
      byte[] converted = key instanceof WrappedByteArray
            ? ((WrappedByteArray) key).getBytes()
            : (byte[]) key;
      return GlobMatcher.match(pattern, converted);
   }

   @Override
   public MediaType format() {
      return null;
   }
}
