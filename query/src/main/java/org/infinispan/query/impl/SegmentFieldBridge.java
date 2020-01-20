package org.infinispan.query.impl;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.hibernate.search.bridge.FieldBridge;
import org.hibernate.search.bridge.LuceneOptions;

/**
 * FieldBridge to index the segment id of the key for each entry.
 *
 * @since 10.1
 */
public class SegmentFieldBridge implements FieldBridge {

   public static final String ID_FIELD = "providedId";
   public static final String SEGMENT_FIELD = "__segmentId";

   public SegmentFieldBridge() {
   }

   @Override
   public void set(String name, Object value, Document document, LuceneOptions luceneOptions) {
      String providedId = document.get(ID_FIELD);
      String segment = providedId.substring(providedId.lastIndexOf(":") + 1);
      document.add(new StringField(SEGMENT_FIELD, segment, Field.Store.NO));
   }
}
