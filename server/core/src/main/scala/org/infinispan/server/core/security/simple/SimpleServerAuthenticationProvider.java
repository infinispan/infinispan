package org.infinispan.server.core.security.simple;

import java.io.IOException;
import java.security.Principal;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthenticationException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

import org.infinispan.server.core.security.AuthorizingCallbackHandler;
import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.infinispan.server.core.security.SubjectUserInfo;

/**
 * A server authentication handler which maintains a simple map of user names and passwords.
 *
 * @author <a href="mailto:darran.lofthouse@jboss.com">Darran Lofthouse</a>
 * @author Tristan Tarrant
 */
public final class SimpleServerAuthenticationProvider implements ServerAuthenticationProvider {

   private final Map<String, Map<String, Entry>> map = new HashMap<String, Map<String, Entry>>();

   /**
    * {@inheritDoc}
    *
    * @param mechanismName
    */
   public AuthorizingCallbackHandler getCallbackHandler(final String mechanismName, final Map<String, String> mechanismProperties) {

      return new AuthorizingCallbackHandler() {
         Subject subject = new Subject();
         Principal userPrincipal;

         public void handle(final Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            String userName = null;
            String realmName = null;
            for (Callback callback : callbacks) {
               if (callback instanceof NameCallback) {
                  final NameCallback nameCallback = (NameCallback) callback;
                  final String defaultName = nameCallback.getDefaultName();
                  userName = defaultName.toLowerCase().trim();
                  nameCallback.setName(userName);
                  userPrincipal = new SimpleUserPrincipal(userName);
                  subject.getPrincipals().add(userPrincipal);
               } else if (callback instanceof RealmCallback) {
                  final RealmCallback realmCallback = (RealmCallback) callback;
                  final String defaultRealm = realmCallback.getDefaultText();
                  if (defaultRealm != null) {
                     realmName = defaultRealm.toLowerCase().trim();
                     realmCallback.setText(realmName);
                  }
               } else if (callback instanceof PasswordCallback) {
                  final PasswordCallback passwordCallback = (PasswordCallback) callback;
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
                     throw new AuthenticationException("No matching user found");
                  }
                  for (String group : entry.getGroups()) {
                     subject.getPrincipals().add(new SimpleGroupPrincipal(group));
                  }
                  passwordCallback.setPassword(entry.getPassword());
               } else if (callback instanceof AuthorizeCallback) {
                  final AuthorizeCallback authorizeCallback = (AuthorizeCallback) callback;
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
            return new SimpleSubjectUserInfo(userPrincipal.getName(), subject);
         }
      };
   }

   /**
    * Add a user to the authentication table.
    *
    * @param userName
    *           the user name
    * @param userRealm
    *           the user realm
    * @param password
    *           the password
    * @param groups
    *           the groups the user belongs to
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
         Map<String, Entry> realmMap = map.get(canonUserRealm);
         if (realmMap == null) {
            realmMap = new HashMap<String, Entry>();
            map.put(canonUserRealm, realmMap);
         }
         realmMap.put(canonUserName, new Entry(canonUserName, canonUserRealm, password, groups != null ? groups : new String[0]));
      }
   }

   private static final class Entry {
      private final String userName;
      private final String userRealm;
      private final char[] password;
      private final String[] groups;

      private Entry(final String userName, final String userRealm, final char[] password, final String[] groups) {
         this.userName = userName;
         this.userRealm = userRealm;
         this.password = password;
         this.groups = groups;
      }

      String getUserName() {
         return userName;
      }

      String getUserRealm() {
         return userRealm;
      }

      char[] getPassword() {
         return password;
      }

      String[] getGroups() {
         return groups;
      }
   }
}
