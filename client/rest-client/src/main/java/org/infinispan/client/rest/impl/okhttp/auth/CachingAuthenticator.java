package org.infinispan.client.rest.impl.okhttp.auth;

import java.io.IOException;

import okhttp3.Authenticator;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.Route;

public interface CachingAuthenticator extends Authenticator {
   Request authenticateWithState(Route route, Request request) throws IOException;

   static String getCachingKey(Request request) {
      final HttpUrl url = request.url();
      if (url == null)
         return null;
      return url.scheme() + ":" + url.host() + ":" + url.port();
   }
}
