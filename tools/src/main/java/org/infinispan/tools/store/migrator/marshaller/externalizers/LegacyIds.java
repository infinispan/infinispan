package org.infinispan.tools.store.migrator.marshaller.externalizers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Legacy Ids used in Infinispan 8 to map {@link org.infinispan.commons.marshall.Externalizer} implementations.
 *
 * Indexes for object types. These are currently limited to being unsigned ints, so valid values are considered those
 * in the range of 0 to 254. Please note that the use of 255 is forbidden since this is reserved for foreign, or user
 * defined, externalizers.
 */
public class LegacyIds {
   public static final Map<Integer, Integer> LEGACY_MAP;
   static {
      HashMap<Integer, Integer> map = new HashMap<>();
      map.put(2, 1); // MAPS
      map.put(10, 7); // IMMORTAL_ENTRY
      map.put(11, 8); // MORTAL_ENTRY
      map.put(12, 9); // TRANSIENT_ENTRY
      map.put(13, 10); // TRANSIENT_MORTAL_ENTRY
      map.put(14, 11); // IMMORTAL_VALUE
      map.put(15, 12); // MORTAL_VALUE
      map.put(16, 13); // TRANSIENT_VALUE
      map.put(17, 14); // TRANSIENT_VALUE
      map.put(19, 105); // IMMUTABLE_MAP
      map.put(76, 38); // METADATA_IMMORTAL_ENTRY
      map.put(77, 39); // METADATA_MORTAL_ENTRY
      map.put(78, 40); // METADATA_TRANSIENT_ENTRY
      map.put(79, 41); // METADATA_TRANSIENT_MORTAL_ENTRY
      map.put(80, 42); // METADATA_IMMORTAL_ENTRY
      map.put(81, 43); // METADATA_MORTAL_ENTRY
      map.put(82, 44); // METADATA_TRANSIENT_VALUE
      map.put(83, 45); // METADATA_TRANSIENT_MORTAL_VALUE
      map.put(96, 55); // SIMPLE_CLUSTERED_VERSION
      map.put(98, 57); // EMBEDDED_METADATA
      map.put(99, 58); // NUMERIC_VERSION
      map.put(103, 60); // KEY_VALUE_PAIR
      map.put(104, 61); // INTERNAL_METADATA
      map.put(105, 62); // MARSHALLED_ENTRY
      map.put(106, 106); // BYTE_BUFFER
      map.put(121, 63); // ENUM_SET
      LEGACY_MAP = Collections.unmodifiableMap(map);
   }

   static int ARRAY_LIST = 0;
   static int JDK_SETS = 3;
   static int SINGLETON_LIST = 4;
   static int IMMUTABLE_LIST = 18;
   static int LIST_ARRAY = 122;
}
