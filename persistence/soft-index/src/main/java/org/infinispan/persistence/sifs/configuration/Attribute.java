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
   COMPACTION_THRESHOLD("compaction-threshold"),
   PATH("path"),
   INDEX_QUEUE_LENGTH("max-queue-length"),
   SEGMENTS("segments"),
   MAX_FILE_SIZE("max-file-size"),
   MAX_NODE_SIZE("max-node-size"),
   MIN_NODE_SIZE("min-node-size"),
   OPEN_FILES_LIMIT("open-files-limit"),
   SYNC_WRITES("sync-writes")
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
