package org.infinispan.server.security;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;

public enum HostnameVerificationPolicy {
   ANY((s, sslSession) -> true),
   DEFAULT(HttpsURLConnection.getDefaultHostnameVerifier());

   private final HostnameVerifier verifier;

   HostnameVerificationPolicy(HostnameVerifier verifier) {
      this.verifier = verifier;
   }

   public HostnameVerifier getVerifier() {
      return verifier;
   }
}
