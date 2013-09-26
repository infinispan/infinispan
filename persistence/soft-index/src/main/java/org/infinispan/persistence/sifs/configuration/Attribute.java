package org.infinispan.persistence.sifs.configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Enumerates the attributes used by the LevelDB cache stores configuration
 * 
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 */
public enum Attribute {
   // must be first
   UNKNOWN(null),
   COMPACTION_THRESHOLD("compactionThreshold"),
   DATA_LOCATION("dataLocation"),
   INDEX_QUEUE_LENGTH("indexQueueLength"),
   INDEX_LOCATION("indexLocation"),
   INDEX_SEGMENTS("indexSegments"),
   MAX_FILE_SIZE("maxFileSize"),
   MAX_NODE_SIZE("maxNodeSize"),
   MIN_NODE_SIZE("minNodeSize"),
   OPEN_FILES_LIMIT("openFilesLimit"),
   SYNC_WRITES("syncWrites")
   ;

   private final String name;

   private Attribute(final String name) {
      this.name = name;
   }

   /**
    * Get the local name of this element.
    * 
    * @return the local name
    */
   public String getLocalName() {
      return name;
   }

   private static final Map<String, Attribute> attributes;

   static {
      final Map<String, Attribute> map = new HashMap<String, Attribute>(64);
      for (Attribute attribute : values()) {
         final String name = attribute.getLocalName();
         if (name != null) {
            map.put(name, attribute);
         }
      }
      attributes = map;
   }

   public static Attribute forName(final String localName) {
      final Attribute attribute = attributes.get(localName);
      return attribute == null ? UNKNOWN : attribute;
   }
}
