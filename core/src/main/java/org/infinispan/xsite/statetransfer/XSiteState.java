package org.infinispan.xsite.statetransfer;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.Metadata;

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

   private XSiteState(Object key, Object value, Metadata metadata) {
      this.key = key;
      this.value = value;
      this.metadata = metadata;
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

   public static XSiteState fromDataContainer(InternalCacheEntry entry) {
      return new XSiteState(entry.getKey(), entry.getValue(), entry.getMetadata());
   }

   public static XSiteState fromCacheLoader(MarshalledEntry marshalledEntry) {
      return new XSiteState(marshalledEntry.getKey(), marshalledEntry.getValue(), marshalledEntry.getMetadata());
   }

   @Override
   public String toString() {
      return "XSiteState{" +
            "key=" + key +
            ", value=" + value +
            ", metadata=" + metadata +
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
      public void writeObject(UserObjectOutput output, XSiteState object) throws IOException {
         output.writeUserObject(object.key);
         output.writeUserObject(object.value);
         output.writeUserObject(object.metadata);
      }

      @Override
      public XSiteState readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         return new XSiteState(input.readUserObject(), input.readUserObject(), (Metadata) input.readUserObject());
      }
   }
}
