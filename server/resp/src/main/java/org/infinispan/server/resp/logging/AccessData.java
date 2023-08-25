package org.infinispan.server.resp.logging;

import java.time.temporal.Temporal;

import org.infinispan.security.Security;
import org.infinispan.server.core.transport.ConnectionMetadata;
import org.infinispan.server.resp.RespCommand;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import net.jcip.annotations.Immutable;

/**
 * Access data from a single request.
 * <p>
 * Maintain the information about one request, start and finishing times, affected keys, request and response size,
 * and failure cause, if any. All the data collected is final, only pending a flush to the access log.
 * <p>
 * When access logging is enabled, each request instantiates a new object.
 */
@Immutable
public class AccessData {
   private final Temporal start;
   private final byte[][] keys;
   private final int requestBytes;
   private final int responseBytes;
   private final String principalName;
   private final String operation;
   private final Throwable throwable;

   private AccessData(Temporal start,
                      byte[][] keys,
                      int requestBytes,
                      int responseBytes,
                      String principalName,
                      String operation,
                      Throwable throwable) {
      this.start = start;
      this.keys = keys;
      this.requestBytes = requestBytes;
      this.responseBytes = responseBytes;
      this.principalName = principalName;
      this.operation = operation;
      this.throwable = throwable;
   }

   static AccessData create(ChannelHandlerContext ctx, RespCommand req, Temporal start, byte[][] keys,
                            int requestBytes, int responseBytes, Throwable throwable) {
      ConnectionMetadata metadata = ConnectionMetadata.getInstance(ctx.channel());
      String principalName = Security.getSubjectUserPrincipalName(metadata.subject());

      return new AccessData(start, keys, requestBytes, responseBytes, principalName, req.getName(), throwable);
   }

   public void log(ChannelFuture future) {
      if (throwable == null) {
         RespAccessLogger.success(future, this);
      } else {
         RespAccessLogger.failure(future, this, throwable);
      }
   }

   public Temporal start() {
      return start;
   }

   public byte[][] keys() {
      return keys;
   }

   public int requestBytes() {
      return requestBytes;
   }

   public int responseBytes() {
      return responseBytes;
   }

   public String principalName() {
      return principalName;
   }

   public String operation() {
      return operation;
   }
}
