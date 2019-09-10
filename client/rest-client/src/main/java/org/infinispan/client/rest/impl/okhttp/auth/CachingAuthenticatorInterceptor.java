package org.infinispan.client.rest.impl.okhttp.auth;


import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

import java.io.IOException;
import java.util.Map;

import okhttp3.Connection;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

public class CachingAuthenticatorInterceptor implements Interceptor {
   private final Map<String, CachingAuthenticator> authCache;

   public CachingAuthenticatorInterceptor(Map<String, CachingAuthenticator> authCache) {
      this.authCache = authCache;
   }

   @Override
   public Response intercept(Chain chain) throws IOException {
      final Request request = chain.request();
      final String key = CachingAuthenticator.getCachingKey(request);
      CachingAuthenticator authenticator = authCache.get(key);
      Request authRequest = null;
      Connection connection = chain.connection();
      Route route = connection != null ? connection.route() : null;
      if (authenticator != null) {
         authRequest = authenticator.authenticateWithState(route, request);
      }
      if (authRequest == null) {
         authRequest = request;
      }
      Response response = chain.proceed(authRequest);

      int responseCode = response != null ? response.code() : 0;
      if (authenticator != null && (responseCode == HTTP_UNAUTHORIZED)) {
         if (authCache.remove(key) != null) {
            response.body().close();
            // Force sending a new request without "Authorization" header
            response = chain.proceed(request);
         }
      }
      return response;
   }
}
