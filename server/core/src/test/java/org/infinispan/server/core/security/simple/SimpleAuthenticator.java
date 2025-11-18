package org.infinispan.server.core.security.simple;

import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.x500.X500Principal;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import javax.security.sasl.SaslServerFactory;

import org.infinispan.commons.util.SaslUtils;
import org.infinispan.server.core.security.SubjectUserInfo;
import org.infinispan.server.core.security.UsernamePasswordAuthenticator;
import org.infinispan.server.core.security.external.ExternalSaslServerFactory;
import org.infinispan.server.core.security.sasl.AuthorizingCallbackHandler;
import org.infinispan.server.core.security.sasl.SaslAuthenticator;
import org.infinispan.server.core.security.sasl.SubjectSaslServer;

/**
 * A server authentication handler which maintains a simple map of user names and passwords.
 *
 * @author <a href="mailto:darran.lofthouse@jboss.com">Darran Lofthouse</a>
 * @author Tristan Tarrant
 */
public final class SimpleAuthenticator implements SaslAuthenticator, UsernamePasswordAuthenticator {

   private final Map<String, Map<String, Entry>> map = new HashMap<>();

   public SaslServer createSaslServer(String mechanism, List<Principal> principals, String protocol, String serverName, Map<String, String> props) throws SaslException {
      AuthorizingCallbackHandler callbackHandler = getCallbackHandler();
      if ("EXTERNAL".equals(mechanism)) {
         // Find the X500Principal among the supplied principals
         for (Principal principal : principals) {
            if (principal instanceof X500Principal) {
               ExternalSaslServerFactory factory = new ExternalSaslServerFactory((X500Principal) principal);
               SaslServer saslServer = factory.createSaslServer(mechanism, protocol, serverName, props, callbackHandler);
               return new SubjectSaslServer(saslServer, principals, callbackHandler);
            }
         }
         throw new IllegalStateException("EXTERNAL mech requires X500Principal");
      } else {
         for (SaslServerFactory factory : SaslUtils.getSaslServerFactories(this.getClass().getClassLoader(), null, true)) {
            if (factory != null) {
               SaslServer saslServer = factory.createSaslServer(mechanism, protocol, serverName, props, callbackHandler);
               if (saslServer != null) {
                  return new SubjectSaslServer(saslServer, principals, callbackHandler);
               }
            }
         }
      }
      return null;
   }

   private AuthorizingCallbackHandler getCallbackHandler() {
      return new AuthorizingCallbackHandler() {
         final Subject subject = new Subject();
         Principal userPrincipal;

         public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            String userName = null;
            String realmName = null;
            for (Callback callback : callbacks) {
               if (callback instanceof NameCallback nameCallback) {
                  final String defaultName = nameCallback.getDefaultName();
                  userName = defaultName.toLowerCase().trim();
                  nameCallback.setName(userName);
                  userPrincipal = new SimpleUserPrincipal(userName);
                  subject.getPrincipals().add(userPrincipal);
               } else if (callback instanceof RealmCallback realmCallback) {
                  final String defaultRealm = realmCallback.getDefaultText();
                  if (defaultRealm != null) {
                     realmName = defaultRealm.toLowerCase().trim();
                     realmCallback.setText(realmName);
                  }
               } else if (callback instanceof RealmChoiceCallback realmChoiceCallback) {
                  realmChoiceCallback.setSelectedIndex(realmChoiceCallback.getDefaultChoice());
               } else if (callback instanceof PasswordCallback passwordCallback) {
                  // retrieve the record based on user and realm (if any)
                  Entry entry = null;
                  if (realmName == null) {
                     // scan all realms
                     synchronized (map) {
                        for (Map<String, Entry> realmMap : map.values()) {
                           if (realmMap.containsKey(userName)) {
                              entry = realmMap.get(userName);
                              break;
                           }
                        }
                     }
                  } else {
                     synchronized (map) {
                        final Map<String, Entry> realmMap = map.get(realmName);
                        if (realmMap != null) {
                           entry = realmMap.get(userName);
                        }
                     }
                  }
                  if (entry == null) {
                     throw new AuthenticationException("Authentication failure");
                  }
                  for (String group : entry.groups()) {
                     subject.getPrincipals().add(new SimpleGroupPrincipal(group));
                  }
                  passwordCallback.setPassword(entry.password());
               } else if (callback instanceof AuthorizeCallback authorizeCallback) {
                  authorizeCallback.setAuthorized(authorizeCallback.getAuthenticationID().equals(
                        authorizeCallback.getAuthorizationID()));
               } else {
                  throw new UnsupportedCallbackException(callback, "Callback not supported: " + callback);
               }
            }
         }

         @Override
         public SubjectUserInfo getSubjectUserInfo(Collection<Principal> principals) {
            if (principals != null) {
               subject.getPrincipals().addAll(principals);
            }
            if (userPrincipal != null) {
               return new SimpleSubjectUserInfo(userPrincipal.getName(), subject);
            } else {
               return new SimpleSubjectUserInfo(subject);
            }
         }
      };
   }

   @Override
   public CompletionStage<Subject> authenticate(String userName, char[] password) {
      final String canonUserName = userName.toLowerCase().trim();
      return map.values().stream()
            .filter(e -> e.containsKey(canonUserName))
            .map(e -> e.get(canonUserName))
            .filter(e -> Arrays.equals(password, e.password))
            .findFirst()
            .map(e -> CompletableFuture.completedFuture(new Subject(true, Set.of(new SimpleUserPrincipal(e.userName)), Set.of(), Set.of())))
            .orElseGet(() -> CompletableFuture.failedFuture(new AuthenticationException("Authentication failure")));
   }

   /**
    * Add a user to the authentication table.
    *
    * @param userName  the user name
    * @param userRealm the user realm
    * @param password  the password
    * @param groups    the groups the user belongs to
    */
   public void addUser(String userName, String userRealm, char[] password, String... groups) {
      if (userName == null) {
         throw new IllegalArgumentException("userName is null");
      }
      if (userRealm == null) {
         throw new IllegalArgumentException("userRealm is null");
      }
      if (password == null) {
         throw new IllegalArgumentException("password is null");
      }
      final String canonUserRealm = userRealm.toLowerCase().trim();
      final String canonUserName = userName.toLowerCase().trim();
      synchronized (map) {
         Map<String, Entry> realmMap = map.computeIfAbsent(canonUserRealm, k -> new HashMap<>());
         realmMap.put(canonUserName, new Entry(canonUserName, canonUserRealm, password, groups != null ? groups : new String[0]));
      }
   }

   private record Entry(String userName, String userRealm, char[] password, String[] groups) {
   }
}
