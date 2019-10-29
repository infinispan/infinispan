package org.infinispan.client.rest.impl.okhttp.auth;

import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;

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

public class NegotiateAuthenticator extends AbstractAuthenticator implements Authenticator {
   private static final String SPNEGO_OID = "1.3.6.1.5.5.2";
   private final AuthenticationConfiguration configuration;
   private final GSSManager gssManager;
   private final Oid oid;

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

   private Request authenticateInternal(Route route, Request request) {
      try {
         // Initial empty challenge
         String host = route.address().url().host();
         byte[] token = generateToken(null, host);
         return request.newBuilder()
               .header(WWW_AUTH_RESP, "Negotiate " + Base64.getEncoder().encodeToString(token))
               .build();
      } catch (GSSException e) {
         throw new AuthenticationException(e.getMessage(), e);
      }
   }

   protected byte[] generateToken(byte[] input, String authServer) throws GSSException {
      GSSName serverName = gssManager.createName("HTTP@" + authServer, GSSName.NT_HOSTBASED_SERVICE);
      GSSContext gssContext = gssManager.createContext(serverName.canonicalize(oid), oid, null, GSSContext.DEFAULT_LIFETIME);
      gssContext.requestMutualAuth(true);
      try {
         return Subject.doAs(configuration.clientSubject(), (PrivilegedExceptionAction<byte[]>) () ->
               input != null
               ? gssContext.initSecContext(input, 0, input.length)
               : gssContext.initSecContext(new byte[]{}, 0, 0)

         );
      } catch (PrivilegedActionException e) {
         throw new SecurityException(e);
      }
   }


}
