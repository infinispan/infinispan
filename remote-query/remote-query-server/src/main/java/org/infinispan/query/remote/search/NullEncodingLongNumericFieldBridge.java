package org.infinispan.query.remote.search;

import org.apache.lucene.document.Document;
import org.hibernate.search.bridge.builtin.LongNumericFieldBridge;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
public class NullEncodingLongNumericFieldBridge extends LongNumericFieldBridge {

   private final String nullMarker;

   public NullEncodingLongNumericFieldBridge(String nullMarker) {
      this.nullMarker = nullMarker;
   }

   @Override
   public Object get(String name, Document document) {
      Object val = super.get(name, document);
      if (nullMarker.equals(val)) {
         val = null;
      }
      return val;
   }

   @Override
   public String objectToString(Object object) {
      if (object == null) {
         return nullMarker;
      } else {
         return super.objectToString(object);
      }
   }
}
