package org.infinispan.server.endpoint.subsystem;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;
import static org.wildfly.security.sasl.util.SaslMechanismInformation.Names.DIGEST_MD5;
import static org.wildfly.security.sasl.util.SaslMechanismInformation.Names.EXTERNAL;
import static org.wildfly.security.sasl.util.SaslMechanismInformation.Names.GSSAPI;
import static org.wildfly.security.sasl.util.SaslMechanismInformation.Names.PLAIN;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
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
import org.wildfly.security.auth.callback.AvailableRealmsCallback;
import org.wildfly.security.sasl.WildFlySasl;

/**
 * EndpointServerAuthenticationProvider.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class EndpointServerAuthenticationProvider implements ServerAuthenticationProvider {
   private static final String SASL_OPT_PRE_DIGESTED_PROPERTY = "org.wildfly.security.sasl.digest.pre_digested";

   private final SecurityRealm realm;
   private String[] realmList;

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
         String realmStr = mechanismProperties.get(WildFlySasl.REALM_LIST);
         realmList = realmStr == null ? new String[] {realm.getName()} : realmStr.split(" ");

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
         for (Callback callback : callbacks) {
            if (callback instanceof AvailableRealmsCallback) {
               ((AvailableRealmsCallback) callback).setRealmNames(realm.getName());
            } else if (callback instanceof AuthorizeCallback) {
               AuthorizeCallback acb = (AuthorizeCallback) callback;
               String authenticationId = acb.getAuthenticationID();
               String authorizationId = acb.getAuthorizationID();
               acb.setAuthorized(authenticationId.equals(authorizationId));
               int realmSep = authorizationId.indexOf('@');
               realmUser = realmSep <= 0 ? new RealmUser(authorizationId) : new RealmUser(authorizationId.substring(realmSep+1), authorizationId.substring(0, realmSep));
            }
         }
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

      RealmAuthorizingCallbackHandler(org.jboss.as.domain.management.AuthorizingCallbackHandler authorizingCallbackHandler) {
         this.delegate = authorizingCallbackHandler;
      }

      @Override
      public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
         ArrayList<Callback> list = new ArrayList<>(Arrays.asList(callbacks));
         Iterator<Callback> it = list.iterator();
         while (it.hasNext()) {
            Callback callback = it.next();
            if (callback instanceof AvailableRealmsCallback) {
               ((AvailableRealmsCallback) callback).setRealmNames(realmList);
               it.remove();
            }
         }

         // If the only callback was AvailableRealmsCallback, we must not pass it to the AuthorizingCallbackHandler
         if (!list.isEmpty()) {
            delegate.handle(callbacks);
         }
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
