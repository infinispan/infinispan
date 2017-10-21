package org.infinispan.rest;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;

/**
 * @since 9.2
 */
public class InfinispanCacheResponse extends InfinispanResponse {

   private final static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.RFC_1123_DATE_TIME.withZone(ZoneId.systemDefault());
   private final static String TIME_TO_LIVE_HEADER = "timeToLiveSeconds";
   private final static String MAX_IDLE_TIME_HEADER = "maxIdleTimeSeconds";
   private final static String CLUSTER_PRIMARY_OWNER_HEADER = "Cluster-Primary-Owner";
   private final static String CLUSTER_NODE_NAME_HEADER = "Cluster-Node-Name";
   private final static String CLUSTER_SERVER_ADDRESS_HEADER = "Cluster-Server-Address";

   private Optional<String> etag = Optional.empty();
   private Optional<CacheControl> cacheControl = Optional.empty();
   private Optional<Date> lastModified = Optional.empty();
   private Optional<Date> expires = Optional.empty();
   private Optional<Long> timeToLive = Optional.empty();
   private Optional<Long> maxIdle = Optional.empty();
   private Optional<String> clusterPrimaryOwner = Optional.empty();
   private Optional<String> clusterNodeName = Optional.empty();
   private Optional<String> clusterServerAddress = Optional.empty();


   private InfinispanCacheResponse(Optional<InfinispanRequest> request) {
      super(request);
   }

   @Override
   protected void addSpecificHeaders(FullHttpResponse response) {
      etag.ifPresent(e -> response.headers().set(HttpHeaderNames.ETAG, e));
      cacheControl.ifPresent(e -> response.headers().set(HttpHeaderNames.CACHE_CONTROL, e));
      lastModified.ifPresent(e -> response.headers().set(HttpHeaderNames.LAST_MODIFIED, DATE_TIME_FORMATTER.format(e.toInstant())));
      expires.ifPresent(e -> response.headers().set(HttpHeaderNames.EXPIRES, DATE_TIME_FORMATTER.format(e.toInstant())));
      timeToLive.ifPresent(e -> response.headers().set(TIME_TO_LIVE_HEADER, TimeUnit.MILLISECONDS.toSeconds(e)));
      maxIdle.ifPresent(e -> response.headers().set(MAX_IDLE_TIME_HEADER, TimeUnit.MILLISECONDS.toSeconds(e)));
      clusterPrimaryOwner.ifPresent(e -> response.headers().set(CLUSTER_PRIMARY_OWNER_HEADER, e));
      clusterNodeName.ifPresent(e -> response.headers().set(CLUSTER_NODE_NAME_HEADER, e));
      clusterServerAddress.ifPresent(e -> response.headers().set(CLUSTER_SERVER_ADDRESS_HEADER, e));
   }

   /**
    * Creates an {@link InfinispanResponse} as a reply to specific {@link InfinispanRequest}.
    *
    * @param request Request to reply to.
    * @return Response object.
    */
   public static InfinispanCacheResponse inReplyTo(InfinispanRequest request) {
      return new InfinispanCacheResponse(Optional.of(request));
   }

   /**
    * Adds ETAG.
    *
    * @param etag ETag value to be added.
    */
   public void etag(String etag) {
      this.etag = Optional.of(etag);
   }

   /**
    * Adds Cache Control headers.
    *
    * @param cacheControl Cache Control headers.
    */
   public void cacheControl(CacheControl cacheControl) {
      this.cacheControl = Optional.ofNullable(cacheControl);
   }

   /**
    * Add <code>last-modified</code> header.
    *
    * @param lastModified <code>last-modified</code> header value.
    */
   public void lastModified(Date lastModified) {
      this.lastModified = Optional.ofNullable(lastModified);
   }

   /**
    * Adds <code>expires</code> header.
    *
    * @param expires <code>expires</code> header value.
    */
   public void expires(Date expires) {
      this.expires = Optional.ofNullable(expires);
   }

   public void timeToLive(long lifespan) {
      if (lifespan > -1) {
         this.timeToLive = Optional.of(Long.valueOf(lifespan));
      }
   }

   /**
    * Adds <code>maxIdleTimeSeconds</code> header.
    * @param maxIdle <code>maxIdleTimeSeconds</code> header value.
    */
   public void maxIdle(long maxIdle) {
      if (maxIdle > -1) {
         this.maxIdle = Optional.of(Long.valueOf(maxIdle));
      }
   }

   /**
    * Adds <code>Cluster-Primary-Owner</code> header.
    *
    * @param primaryOwner <code>Cluster-Primary-Owner</code> header value.
    */
   public void clusterPrimaryOwner(String primaryOwner) {
      this.clusterPrimaryOwner = Optional.of(primaryOwner);
   }

   /**
    * Adds <code>Cluster-Node-Name</code> header.
    *
    * @param nodeName <code>Cluster-Node-Name</code> header value.
    */
   public void clusterNodeName(String nodeName) {
      this.clusterNodeName = Optional.of(nodeName);
   }

   /**
    * Adds <code>Cluster-Server-Address</code> header.
    *
    * @param serverAddress <code>Cluster-Server-Address</code> header value.
    */
   public void clusterServerAddress(String serverAddress) {
      this.clusterServerAddress = Optional.of(serverAddress);
   }


}
