package org.infinispan.rest;

import java.util.Arrays;

/**
 * @since 11.0
 */
public enum RequestHeader {
   AUTHORIZATION("Authorization"),
   CACHE_CONTROL_HEADER("Cache-Control"),
   CONTENT_TYPE_HEADER("Content-Type"),
   CREATED_HEADER("created"),
   EXTENDED_HEADER("extended"),
   FLAGS_HEADER("flags"),
   IF_MODIFIED_SINCE("If-Modified-Since"),
   IF_NONE_MATCH("If-None-Match"),
   IF_UNMODIFIED_SINCE("If-UnModified-Since"),
   KEY_CONTENT_TYPE_HEADER("key-content-type"),
   LAST_USED_HEADER("lastUsed"),
   MAX_TIME_IDLE_HEADER("maxIdleTimeSeconds"),
   TTL_SECONDS_HEADER("timeToLiveSeconds");

   private static final CharSequence[] ALL_VALUES = Arrays.stream(values()).map(RequestHeader::getValue).toArray(String[]::new);

   private final String value;

   RequestHeader(String value) {
      this.value = value;
   }

   public String getValue() {
      return value;
   }

   public static CharSequence[] toArray() {
      return ALL_VALUES;
   }

}
