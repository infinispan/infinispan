package org.infinispan.xsite.statetransfer;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.persistence.spi.MarshallableEntry;

/**
 * Represents the state of a single key to be sent to a backup site. It contains the only needed information, i.e., the
 * key, current value and associated metadata.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
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

   public final Object key() {
      return key;
   }

   public final Object value() {
      return value;
   }

   public final Metadata metadata() {
      return metadata;
   }

   public PrivateMetadata internalMetadata() {
      return internalMetadata;
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

   public static class XSiteStateExternalizer extends AbstractExternalizer<XSiteState> {

      @Override
      public Integer getId() {
         return Ids.X_SITE_STATE;
      }

      @Override
      public Set<Class<? extends XSiteState>> getTypeClasses() {
         return Collections.singleton(XSiteState.class);
      }

      @Override
      public void writeObject(ObjectOutput output, XSiteState object) throws IOException {
         output.writeObject(object.key);
         output.writeObject(object.value);
         output.writeObject(object.metadata);
         output.writeObject(object.internalMetadata);
      }

      @Override
      public XSiteState readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new XSiteState(input.readObject(), input.readObject(), (Metadata) input.readObject(),
               (PrivateMetadata) input.readObject());
      }
   }
}
