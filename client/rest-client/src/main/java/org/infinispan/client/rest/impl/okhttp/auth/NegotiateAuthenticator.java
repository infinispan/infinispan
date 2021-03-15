package org.infinispan.client.rest.impl.okhttp.auth;

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.infinispan.client.rest.configuration.AuthenticationConfiguration;

import okhttp3.Authenticator;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

public class NegotiateAuthenticator extends AbstractAuthenticator implements CachingAuthenticator {
   private static final String SPNEGO_OID = "1.3.6.1.5.5.2";
   private final AuthenticationConfiguration configuration;
   private final GSSManager gssManager;
   private final Oid oid;
   private final AtomicReference<String> token = new AtomicReference<>();

   public NegotiateAuthenticator(AuthenticationConfiguration configuration) {
      this.configuration = configuration;
      this.gssManager = GSSManager.getInstance();
      try {
         this.oid = new Oid(SPNEGO_OID);
      } catch (GSSException e) {
         throw new RuntimeException(e); // This will never happen
      }
   }

   @Override
   public Request authenticate(Route route, Response response) {
      Request request = response.request();
      return authenticateInternal(route, request);
   }

   @Override
   public Request authenticateWithState(Route route, Request request) throws IOException {
      return request.newBuilder()
            .header(WWW_AUTH_RESP, "Negotiate " + token.get())
            .build();
   }

   private Request authenticateInternal(Route route, Request request) {
      try {
         // Initial empty challenge
         String host = route.address().url().host();
         token.set(generateToken(null, host));
         return request.newBuilder()
               .header(WWW_AUTH_RESP, "Negotiate " + token.get())
               .tag(Authenticator.class, this)
               .build();
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
               input != null
                     ? gssContext.initSecContext(input, 0, input.length)
                     : gssContext.initSecContext(new byte[]{}, 0, 0)

         );
         return Base64.getEncoder().encodeToString(bytes);
      } catch (PrivilegedActionException e) {
         throw new SecurityException(e);
      }
   }
}
