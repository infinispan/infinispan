package org.infinispan.xsite.statetransfer;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Represents the state of a single key to be sent to a backup site. It contains the only needed information, i.e., the
 * key, current value and associated metadata.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.XSITE_STATE)
public class XSiteState {

   private final Object key;
   private final Object value;
   private final Metadata metadata;
   private final PrivateMetadata internalMetadata;

   private XSiteState(Object key, Object value, Metadata metadata, PrivateMetadata internalMetadata) {
      this.key = key;
      this.value = value;
      this.metadata = metadata;
      this.internalMetadata = internalMetadata;
   }

   @ProtoFactory
   XSiteState(MarshallableObject<?> key, MarshallableObject<?> value, MarshallableObject<Metadata> metadata,
              PrivateMetadata internalMetadata) {
      this(MarshallableObject.unwrap(key), MarshallableObject.unwrap(value),
            MarshallableObject.unwrap(metadata), internalMetadata);
   }

   @ProtoField(1)
   MarshallableObject<?> getKey() {
      return new MarshallableObject<>(key);
   }

   @ProtoField(2)
   MarshallableObject<?> getValue() {
      return new MarshallableObject<>(value);
   }

   @ProtoField(3)
   MarshallableObject<Metadata> getMetadata() {
      return new MarshallableObject<>(metadata);
   }

   @ProtoField(4)
   public PrivateMetadata internalMetadata() {
      return internalMetadata;
   }

   public final Object key() {
      return key;
   }

   public final Object value() {
      return value;
   }

   public final Metadata metadata() {
      return metadata;
   }

   public static XSiteState fromDataContainer(InternalCacheEntry<?, ?> entry) {
      return new XSiteState(entry.getKey(), entry.getValue(), entry.getMetadata(), entry.getInternalMetadata());
   }

   public static XSiteState fromCacheLoader(MarshallableEntry<?, ?> marshalledEntry) {
      return new XSiteState(marshalledEntry.getKey(), marshalledEntry.getValue(), marshalledEntry.getMetadata(),
            marshalledEntry.getInternalMetadata());
   }

   @Override
   public String toString() {
      return "XSiteState{" +
            "key=" + Util.toStr(key) +
            ", value=" + Util.toStr(value) +
            ", metadata=" + metadata +
            ", internalMetadata=" + internalMetadata +
            '}';
   }
}
