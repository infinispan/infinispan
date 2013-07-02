package org.infinispan.container.versioning;

import java.util.HashMap;

public class EntryVersionsMap extends HashMap<Object, IncrementableEntryVersion> {
   public EntryVersionsMap merge(EntryVersionsMap updatedVersions) {
      if (updatedVersions != null && !updatedVersions.isEmpty()) {
         updatedVersions.putAll(this);
         return updatedVersions;
      } else {
         return this;
      }
   }
}
