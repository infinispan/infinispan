package org.infinispan.rest;

import java.util.Map;

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.util.internal.StringUtil;

/**
 * @author Ryan Emerson
 * @since 14.0
 */
final class HttpMessageUtil {
   static String dumpRequest(HttpRequest req) {
      StringBuilder sb = new StringBuilder();
      sb.append(req.method()).append(' ').append(req.uri()).append(' ').append(req.protocolVersion());
      appendHeaders(sb, req.headers());
      return sb.toString();
   }

   static String dumpResponse(HttpResponse res) {
      StringBuilder sb = new StringBuilder();
      sb.append(res.protocolVersion()).append(' ').append(res.status());
      appendHeaders(sb, res.headers());
      return sb.toString();
   }

   private static void appendHeaders(StringBuilder buf, HttpHeaders headers) {
      for (Map.Entry<String, String> header : headers) {
         buf.append(StringUtil.NEWLINE);
         buf.append(header.getKey());
         buf.append(": ");
         buf.append(header.getValue());
      }
   }

   private HttpMessageUtil() {
   }
}
