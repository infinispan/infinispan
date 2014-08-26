package org.infinispan.server.endpoint.subsystem;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;

import org.infinispan.server.core.security.AuthorizingCallbackHandler;
import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.infinispan.server.core.security.SubjectUserInfo;
import org.jboss.as.core.security.RealmUser;
import org.jboss.as.domain.management.AuthMechanism;
import org.jboss.as.domain.management.RealmConfigurationConstants;
import org.jboss.as.domain.management.SecurityRealm;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;
/**
 * EndpointServerAuthenticationProvider.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class EndpointServerAuthenticationProvider implements ServerAuthenticationProvider {
   static final String SASL_OPT_REALM_PROPERTY = "com.sun.security.sasl.digest.realm";
   static final String SASL_OPT_ALT_PROTO_PROPERTY = "org.jboss.sasl.digest.alternative_protocols";
   static final String SASL_OPT_PRE_DIGESTED_PROPERTY = "org.jboss.sasl.digest.pre_digested";

   static final String DIGEST_MD5 = "DIGEST-MD5";
   static final String EXTERNAL = "EXTERNAL";
   static final String GSSAPI = "GSSAPI";
   static final String PLAIN = "PLAIN";


   private final SecurityRealm realm;

   EndpointServerAuthenticationProvider(SecurityRealm realm) {
      this.realm = realm;
   }

   @Override
   public AuthorizingCallbackHandler getCallbackHandler(String mechanismName, Map<String, String> mechanismProperties) {
      if (GSSAPI.equals(mechanismName)) {
         // The EAP SecurityRealm doesn't actually support a GSSAPI mech yet so let's handle this ourselves
         return new GSSAPIEndpointAuthorizingCallbackHandler();
      } else if (PLAIN.equals(mechanismName)) {
         return new RealmAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.PLAIN));
      } else if (DIGEST_MD5.equals(mechanismName)) {
         if (!mechanismProperties.containsKey(SASL_OPT_REALM_PROPERTY)) {
            mechanismProperties.put(SASL_OPT_REALM_PROPERTY, realm.getName());
         }
         Map<String, String> mechConfig = realm.getMechanismConfig(AuthMechanism.DIGEST);
         boolean plainTextDigest = true;
         if (mechConfig.containsKey(RealmConfigurationConstants.DIGEST_PLAIN_TEXT)) {
             plainTextDigest = Boolean.parseBoolean(mechConfig.get(RealmConfigurationConstants.DIGEST_PLAIN_TEXT));
         }
         if (!plainTextDigest) {
            mechanismProperties.put(SASL_OPT_PRE_DIGESTED_PROPERTY, "true");
         }
         return new RealmAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.DIGEST));
      } else if (EXTERNAL.equals(mechanismName)) {
         return new RealmAuthorizingCallbackHandler(realm.getAuthorizingCallbackHandler(AuthMechanism.CLIENT_CERT));
      } else {
         throw new IllegalArgumentException("Unsupported mech " + mechanismName);
      }
   }

   public class GSSAPIEndpointAuthorizingCallbackHandler implements AuthorizingCallbackHandler {
      private final org.jboss.as.domain.management.AuthorizingCallbackHandler delegate;
      private RealmUser realmUser;

      GSSAPIEndpointAuthorizingCallbackHandler() {
          delegate = realm.getAuthorizingCallbackHandler(AuthMechanism.PLAIN);
      }

      @Override
      public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
         AuthorizeCallback acb = (AuthorizeCallback) callbacks[0];
         String authenticationId = acb.getAuthenticationID();
         String authorizationId = acb.getAuthorizationID();
         acb.setAuthorized(authenticationId.equals(authorizationId));
         int realmSep = authorizationId.indexOf('@');
         realmUser = realmSep <= 0 ? new RealmUser(authorizationId) : new RealmUser(authorizationId.substring(realmSep+1), authorizationId.substring(0, realmSep));
      }

      @Override
      public SubjectUserInfo getSubjectUserInfo(Collection<Principal> principals) {
          // The call to the delegate will supplement the realm user with additional role information
          Collection<Principal> realmPrincipals = new ArrayList<>();
          realmPrincipals.add(realmUser);
          try {
              org.jboss.as.core.security.SubjectUserInfo userInfo = delegate.createSubjectUserInfo(realmPrincipals);
              userInfo.getPrincipals().addAll(principals);
              return new RealmSubjectUserInfo(userInfo);
          } catch (IOException e) {
              throw ROOT_LOGGER.cannotRetrieveAuthorizationInformation(e, realmUser.toString());
          }
      }
   }

   public class RealmAuthorizingCallbackHandler implements AuthorizingCallbackHandler {

      private final org.jboss.as.domain.management.AuthorizingCallbackHandler delegate;

      public RealmAuthorizingCallbackHandler(org.jboss.as.domain.management.AuthorizingCallbackHandler authorizingCallbackHandler) {
         this.delegate = authorizingCallbackHandler;
      }

      @Override
      public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
         delegate.handle(callbacks);
      }

      @Override
      public SubjectUserInfo getSubjectUserInfo(Collection<Principal> principals) {
         try {
            org.jboss.as.core.security.SubjectUserInfo realmUserInfo = delegate.createSubjectUserInfo(principals);
            return new RealmSubjectUserInfo(realmUserInfo.getUserName(), realmUserInfo.getSubject());
         } catch (IOException e) {
            // Handle this
            return null;
         }

      }

   }
}
