package org.infinispan.rest;

import java.util.Arrays;

/**
 * @since 11.0
 */
public enum ResponseHeader {
   CACHE_CONTROL_HEADER("Cache-Control"),
   CLUSTER_PRIMARY_OWNER_HEADER("Cluster-Primary-Owner"),
   CLUSTER_BACKUP_OWNERS_HEADER("Cluster-Backup-Owners"),
   CLUSTER_NODE_NAME_HEADER("Cluster-Node-Name"),
   CLUSTER_SERVER_ADDRESS_HEADER("Cluster-Server-Address"),
   CONTENT_LENGTH_HEADER("Content-Length"),
   CONTENT_TYPE_HEADER("Content-Type"),
   CREATED_HEADER("created"),
   DATE_HEADER("Date"),
   ETAG_HEADER("Etag"),
   EXPIRES_HEADER("Expires"),
   LAST_MODIFIED_HEADER("Last-Modified"),
   LAST_USED_HEADER("lastUsed"),
   LOCATION("location"),
   MAX_IDLE_TIME_HEADER("maxIdleTimeSeconds"),
   TIME_TO_LIVE_HEADER("timeToLiveSeconds"),
   TRANSFER_ENCODING("Transfer-Encoding"),
   WWW_AUTHENTICATE_HEADER("WWW-Authenticate");

   private static final CharSequence[] ALL_VALUES = Arrays.stream(values()).map(ResponseHeader::getValue).toArray(String[]::new);

   private final String value;

   ResponseHeader(String value) {
      this.value = value;
   }

   public String getValue() {
      return value;
   }

   public static CharSequence[] toArray() {
      return ALL_VALUES;
   }
}
