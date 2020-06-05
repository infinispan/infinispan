package org.infinispan.rest;

import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.HEAD;
import static io.netty.handler.codec.http.HttpMethod.OPTIONS;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;

import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.infinispan.rest.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;

/**
 * @since 11.0
 */
class CorsUtil {
   static final Log LOG = LogFactory.getLog(CorsUtil.class, Log.class);
   static final String[] SCHEMES = new String[]{"http", "https"};
   static final String ENABLE_ALL_FOR_ORIGIN_PROPERTY = "infinispan.server.rest.cors-allow";

   private static final List<CorsConfig> SYSTEM_CONFIG = new ArrayList<>();

   static {
      String originProp = System.getProperty(ENABLE_ALL_FOR_ORIGIN_PROPERTY);
      if (originProp != null) {
         Arrays.stream(originProp.split(","))
               .map(s -> s.replaceAll("\\s", ""))
               .filter(CorsUtil::isValidOrigin)
               .map(CorsUtil::enableAll)
               .forEach(SYSTEM_CONFIG::add);
      }
   }

   static List<CorsConfig> enableAllForSystemConfig() {
      return SYSTEM_CONFIG;
   }

   static List<CorsConfig> enableAllForLocalHost(int... ports) {
      List<CorsConfig> configs = new ArrayList<>();
      for (int port : ports) {
         for (String scheme : SCHEMES) {
            String localIpv4 = scheme + "://" + "127.0.0.1" + ":" + port;
            String localDomain = scheme + "://" + "localhost" + ":" + port;
            String localIpv6 = scheme + "://" + "[::1]" + ":" + port;
            configs.add(enableAll(localIpv4));
            configs.add(enableAll(localIpv6));
            configs.add(enableAll(localDomain));
         }
      }
      return Collections.unmodifiableList(configs);
   }


   private static boolean isValidOrigin(String prop) {
      try {
         new URL(prop).toURI();
      } catch (URISyntaxException | MalformedURLException e) {
         LOG.invalidOrigin(prop, ENABLE_ALL_FOR_ORIGIN_PROPERTY);
         return false;
      }
      return true;
   }

   /**
    * @return a {@link CorsConfig} with all permissions for the provided origins.
    */
   private static CorsConfig enableAll(String... origins) {
      return CorsConfigBuilder.forOrigins(origins)
            .allowCredentials()
            .allowedRequestMethods(GET, POST, PUT, DELETE, HEAD, OPTIONS)
            // Not all browsers support "*" (https://github.com/whatwg/fetch/issues/251) so we need to add each
            // header individually
            .allowedRequestHeaders(RequestHeader.toArray())
            .exposeHeaders(ResponseHeader.toArray())
            .build();
   }
}
