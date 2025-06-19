package org.infinispan.client.openapi.impl.jdk.auth;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.infinispan.client.openapi.configuration.AuthenticationConfiguration;

public class NegotiateAuthenticator extends HttpAuthenticator {
   private static final String SPNEGO_OID = "1.3.6.1.5.5.2";
   public static final byte[] EMPTY = {};
   private final GSSManager gssManager;
   private final Oid oid;
   private final AtomicReference<String> token = new AtomicReference<>();

   public NegotiateAuthenticator(HttpClient client, AuthenticationConfiguration configuration) {
      super(client, configuration);
      this.gssManager = GSSManager.getInstance();
      try {
         this.oid = new Oid(SPNEGO_OID);
      } catch (GSSException e) {
         throw new RuntimeException(e); // This will never happen
      }
   }

   @Override
   public <T> CompletionStage<HttpResponse<T>> authenticate(HttpResponse<T> response, HttpResponse.BodyHandler<?> bodyHandler) {
      try {
         HttpRequest request = response.request();
         String host = request.uri().getHost();
         token.set(generateToken(EMPTY, host));
         HttpRequest.Builder newRequest = copyRequest(request, (n, v) -> true).header(WWW_AUTH_RESP, "Negotiate " + token.get());
         return client.sendAsync(newRequest.build(), (HttpResponse.BodyHandler<T>) bodyHandler);
      } catch (GSSException e) {
         throw new AuthenticationException(e.getMessage(), e);
      }
   }

   protected String generateToken(byte[] input, String authServer) throws GSSException {
      GSSName serverName = gssManager.createName("HTTP@" + authServer, GSSName.NT_HOSTBASED_SERVICE);
      GSSContext gssContext = gssManager.createContext(serverName.canonicalize(oid), oid, null, GSSContext.DEFAULT_LIFETIME);
      gssContext.requestMutualAuth(true);
      try {
         byte[] bytes = Subject.doAs(configuration.clientSubject(), (PrivilegedExceptionAction<byte[]>) () ->
               gssContext.initSecContext(input, 0, input.length)
         );
         return Base64.getEncoder().encodeToString(bytes);
      } catch (PrivilegedActionException e) {
         throw new SecurityException(e);
      }
   }
}
