package org.infinispan.commons.internal;

import java.util.Set;

public final class InternalCacheNames {

   private InternalCacheNames() {}

   // stores cluster-wide configuration
   public static final String CONFIG_STATE_CACHE_NAME = "org.infinispan.CONFIG";
   // stores cluster-wide protobuf schemas
   public static final String PROTOBUF_METADATA_CACHE_NAME = "___protobuf_metadata";
   // stores cluster-wide server scripts
   public static final String SCRIPT_CACHE_NAME = "___script_cache";

   public static final Set<String> GLOBAL_STATE_INTERNAL_CACHES = Set.of(
         CONFIG_STATE_CACHE_NAME,
         PROTOBUF_METADATA_CACHE_NAME,
         SCRIPT_CACHE_NAME
   );

}
