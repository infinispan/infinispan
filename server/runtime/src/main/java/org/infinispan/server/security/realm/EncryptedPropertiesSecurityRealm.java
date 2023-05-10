package org.infinispan.server.security.realm;

import static org.infinispan.server.Server.log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.Provider;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.infinispan.server.Server;
import org.infinispan.server.security.ElytronPasswordProviderSupplier;
import org.wildfly.common.Assert;
import org.wildfly.common.iteration.CodePointIterator;
import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.principal.NamePrincipal;
import org.wildfly.security.auth.realm.CacheableSecurityRealm;
import org.wildfly.security.auth.server.ModifiableRealmIdentity;
import org.wildfly.security.auth.server.ModifiableRealmIdentityIterator;
import org.wildfly.security.auth.server.ModifiableSecurityRealm;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.auth.server.SecurityRealm;
import org.wildfly.security.authz.Attributes;
import org.wildfly.security.authz.AuthorizationIdentity;
import org.wildfly.security.authz.MapAttributes;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.evidence.Evidence;
import org.wildfly.security.evidence.PasswordGuessEvidence;
import org.wildfly.security.password.PasswordFactory;
import org.wildfly.security.password.spec.BasicPasswordSpecEncoding;
import org.wildfly.security.password.spec.ClearPasswordSpec;
import org.wildfly.security.password.spec.PasswordSpec;

/**
 * A {@link SecurityRealm} implementation that uses property files with encrypted passwords
 *
 * @author Darran Lofthouse &lt;darran.lofthouse@jboss.com&gt;
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 */
public class EncryptedPropertiesSecurityRealm implements CacheableSecurityRealm, ModifiableSecurityRealm {

   private static final String COMMENT_PREFIX1 = "#";
   private static final String COMMENT_PREFIX2 = "!";
   private static final String REALM_COMMENT_PREFIX = "$REALM_NAME=";
   private static final String COMMENT_SUFFIX = "$";
   private static final String ALGORITHM_COMMENT_PREFIX = "$ALGORITHM=";

   private final Supplier<Provider[]> providers;
   private final String defaultRealm;
   private final boolean plainText;
   private final String groupsAttribute;
   private final AtomicReference<LoadedState> loadedState = new AtomicReference<>();
   private Set<Consumer<Principal>> listeners = new LinkedHashSet<>();

   private EncryptedPropertiesSecurityRealm(Builder builder) {
      plainText = builder.plainText;
      groupsAttribute = builder.groupsAttribute;
      providers = ElytronPasswordProviderSupplier.INSTANCE;
      defaultRealm = builder.defaultRealm;
      try {
         load(null, null);
      } catch (IOException e) {
         Server.log.debugf(e, "Error while loading properties");
      }
   }

   @Override
   public RealmIdentity getRealmIdentity(final Principal principal) {
      if (!(principal instanceof NamePrincipal)) {
         log.tracef("PropertiesRealm: unsupported principal type: [%s]", principal);
         return RealmIdentity.NON_EXISTENT;
      }
      final LoadedState loadedState = this.loadedState.get();

      final AccountEntry accountEntry = loadedState.getAccounts().get(principal.getName());

      if (accountEntry == null) {
         log.tracef("PropertiesRealm: identity [%s] does not exist", principal);
         return RealmIdentity.NON_EXISTENT;
      }

      return new RealmIdentity() {

         @Override
         public Principal getRealmIdentityPrincipal() {
            return principal;
         }

         @Override
         public SupportLevel getCredentialAcquireSupport(final Class<? extends Credential> credentialType, final String algorithmName, final AlgorithmParameterSpec parameterSpec) {
            for (Credential credential : accountEntry.getCredentials()) {
               if (credential != null && credential.matches(credentialType, algorithmName, parameterSpec)) {
                  return SupportLevel.SUPPORTED;
               }
            }
            return SupportLevel.UNSUPPORTED;
         }

         @Override
         public SupportLevel getEvidenceVerifySupport(final Class<? extends Evidence> evidenceType, final String algorithmName) {
            for (Credential credential : accountEntry.getCredentials()) {
               if (credential != null && credential.canVerify(evidenceType, algorithmName)) {
                  return SupportLevel.SUPPORTED;
               }
            }
            return SupportLevel.UNSUPPORTED;
         }

         @Override
         public <C extends Credential> C getCredential(final Class<C> credentialType) {
            return getCredential(credentialType, null);
         }

         @Override
         public <C extends Credential> C getCredential(final Class<C> credentialType, final String algorithmName) {
            return getCredential(credentialType, algorithmName, null);
         }

         @Override
         public <C extends Credential> C getCredential(final Class<C> credentialType, final String algorithmName, final AlgorithmParameterSpec parameterSpec) {
            for (Credential credential : accountEntry.getCredentials()) {
               if (credential != null && credential.matches(credentialType, algorithmName, parameterSpec)) {
                  return credentialType.cast(credential.clone());
               }
            }
            return null;
         }

         @Override
         public boolean verifyEvidence(final Evidence evidence) {
            for (Credential credential : accountEntry.getCredentials()) {
               if (credential != null && credential.canVerify(evidence)) {
                  return credential.verify(evidence);
               }
            }
            log.tracef("Unable to verify evidence for identity [%s]", principal);
            return false;

         }

         @Override
         public boolean exists() {
            return true;
         }

         @Override
         public AuthorizationIdentity getAuthorizationIdentity() {
            return AuthorizationIdentity.basicIdentity(new MapAttributes(Collections.singletonMap(groupsAttribute, accountEntry.getGroups())));
         }
      };
   }

   private PasswordFactory getPasswordFactory(final String algorithm) {
      try {
         return PasswordFactory.getInstance(algorithm, providers);
      } catch (NoSuchAlgorithmException e) {
         throw new IllegalStateException(e);
      }
   }

   @Override
   public SupportLevel getCredentialAcquireSupport(final Class<? extends Credential> credentialType, final String algorithmName, final AlgorithmParameterSpec parameterSpec) {
      Assert.checkNotNullParam("credentialType", credentialType);
      return PasswordCredential.class.isAssignableFrom(credentialType) ? SupportLevel.POSSIBLY_SUPPORTED : SupportLevel.UNSUPPORTED;
   }

   @Override
   public SupportLevel getEvidenceVerifySupport(final Class<? extends Evidence> evidenceType, final String algorithmName) {
      return PasswordGuessEvidence.class.isAssignableFrom(evidenceType) ? SupportLevel.SUPPORTED : SupportLevel.UNSUPPORTED;
   }

   /**
    * Loads this properties security realm from the given user and groups input streams.
    *
    * @param usersStream  the input stream from which the realm users are loaded
    * @param groupsStream the input stream from which the roles of realm users are loaded
    * @throws IOException if there is problem while reading the input streams or invalid content is loaded from streams
    */
   public void load(InputStream usersStream, InputStream groupsStream) throws IOException {
      Map<String, AccountEntry> accounts = new HashMap<>();
      Properties groups = new Properties();

      if (groupsStream != null) {
         try (InputStreamReader is = new InputStreamReader(groupsStream, StandardCharsets.UTF_8);) {
            groups.load(is);
         }
      }

      long loadTime = 0;
      String realmName = null;
      String algorithm = "clear";
      if (usersStream != null) {
         try (BufferedReader reader = new BufferedReader(new InputStreamReader(usersStream, StandardCharsets.UTF_8))) {
            String currentLine;
            while ((currentLine = reader.readLine()) != null) {
               final String trimmed = currentLine.trim();
               if (trimmed.startsWith(COMMENT_PREFIX1) && trimmed.contains(REALM_COMMENT_PREFIX)) {
                  // this is the line that contains the realm name.
                  int start = trimmed.indexOf(REALM_COMMENT_PREFIX) + REALM_COMMENT_PREFIX.length();
                  int end = trimmed.indexOf(COMMENT_SUFFIX, start);
                  if (end > -1) {
                     realmName = trimmed.substring(start, end);
                  }
               } else if (trimmed.startsWith(COMMENT_PREFIX1) && trimmed.contains(ALGORITHM_COMMENT_PREFIX)) {
                  // this is the line that contains the algorithm name.
                  int start = trimmed.indexOf(ALGORITHM_COMMENT_PREFIX) + ALGORITHM_COMMENT_PREFIX.length();
                  int end = trimmed.indexOf(COMMENT_SUFFIX, start);
                  if (end > -1) {
                     algorithm = trimmed.substring(start, end);
                  }
               } else {
                  if (!(trimmed.startsWith(COMMENT_PREFIX1) || trimmed.startsWith(COMMENT_PREFIX2))) {
                     String username = null;
                     StringBuilder builder = new StringBuilder();

                     CodePointIterator it = CodePointIterator.ofString(trimmed);
                     while (it.hasNext()) {
                        int cp = it.next();
                        if (cp == '\\' && it.hasNext()) { // escape
                           //might be regular escape of regex like characters \\t \\! or unicode \\uxxxx
                           int marker = it.next();
                           if (marker != 'u') {
                              builder.appendCodePoint(marker);
                           } else {
                              StringBuilder hex = new StringBuilder();
                              try {
                                 hex.appendCodePoint(it.next());
                                 hex.appendCodePoint(it.next());
                                 hex.appendCodePoint(it.next());
                                 hex.appendCodePoint(it.next());
                                 builder.appendCodePoint((char) Integer.parseInt(hex.toString(), 16));
                              } catch (NoSuchElementException nsee) {
                                 throw Server.log.invalidUnicodeSequence(hex.toString(), nsee);
                              }
                           }
                        } else if (username == null && (cp == '=' || cp == ':')) { // username-password delimiter
                           username = builder.toString().trim();
                           builder = new StringBuilder();
                        } else {
                           builder.appendCodePoint(cp);
                        }
                     }
                     if (username != null) { // end of line and delimiter was read
                        List<Credential> credentials = new ArrayList<>();
                        switch (algorithm) {
                           case "encrypted":
                              String[] passwords = builder.toString().trim().split(";");
                              for (String password : passwords) {
                                 int colon = password.indexOf(':');
                                 byte[] passwordBytes = CodePointIterator.ofChars(password.substring(colon + 1).toCharArray()).base64Decode().drain();
                                 PasswordFactory factory = getPasswordFactory(password.substring(0, colon));
                                 PasswordSpec passwordSpec = BasicPasswordSpecEncoding.decode(passwordBytes);
                                 try {
                                    credentials.add(new PasswordCredential(factory.generatePassword(passwordSpec)));
                                 } catch (InvalidKeySpecException e) {
                                    throw new IOException(e);
                                 }
                              }
                              break;
                           case "clear":
                              PasswordFactory factory = getPasswordFactory("clear");
                              try {
                                 credentials.add(new PasswordCredential(factory.generatePassword(new ClearPasswordSpec(builder.toString().trim().toCharArray()))));
                              } catch (InvalidKeySpecException e) {
                                 throw new IOException(e);
                              }
                              break;
                        }

                        accounts.put(username, new AccountEntry(username, credentials, groups.getProperty(username)));
                        // Notify any caching layer that the principal has been reloaded
                        for (Consumer<Principal> listener : listeners) {
                           listener.accept(new NamePrincipal(username));
                        }
                     }
                  }
               }
            }
         }

         if (realmName == null) {
            if (defaultRealm != null || plainText) {
               realmName = defaultRealm;
            } else {
               throw log.noRealmFoundInProperties();
            }
         }
         loadTime = System.currentTimeMillis();
      }

      // users, which are in groups file only
      for (String userName : groups.stringPropertyNames()) {
         if (!accounts.containsKey(userName)) {
            accounts.put(userName, new AccountEntry(userName, null, groups.getProperty(userName)));
         }
      }

      loadedState.set(new LoadedState(accounts, realmName, loadTime));
   }

   /**
    * Get the time when the realm was last loaded.
    *
    * @return the time when the realm was last loaded (number of milliseconds since the standard base time)
    */
   public long getLoadTime() {
      return loadedState.get().getLoadTime();
   }

   /**
    * Obtain a new {@link Builder} capable of building a {@link EncryptedPropertiesSecurityRealm}.
    *
    * @return a new {@link Builder} capable of building a {@link EncryptedPropertiesSecurityRealm}.
    */
   public static Builder builder() {
      return new Builder();
   }

   @Override
   public void registerIdentityChangeListener(Consumer<Principal> listener) {
      listeners.add(listener);
   }

   @Override
   public ModifiableRealmIdentityIterator getRealmIdentityIterator() {
      final LoadedState loadedState = this.loadedState.get();
      Iterator<AccountEntry> accountIterator = loadedState.getAccounts().values().stream().iterator();
      return new ModifiableRealmIdentityIterator() {
         @Override
         public boolean hasNext() {
            return accountIterator.hasNext();
         }

         @Override
         public ModifiableRealmIdentity next() {
            AccountEntry entry = accountIterator.next();
            return new ModifiableRealmIdentity() {
               @Override
               public void delete() {
                  throw new UnsupportedOperationException();
               }

               @Override
               public void create() {
                  throw new UnsupportedOperationException();
               }

               @Override
               public void setCredentials(Collection<? extends Credential> credentials) {
                  throw new UnsupportedOperationException();
               }

               @Override
               public void setAttributes(Attributes attributes) {
                  throw new UnsupportedOperationException();
               }

               @Override
               public Principal getRealmIdentityPrincipal() {
                  return new NamePrincipal(entry.name);
               }

               @Override
               public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName, AlgorithmParameterSpec parameterSpec) throws RealmUnavailableException {
                  throw new UnsupportedOperationException();
               }

               @Override
               public <C extends Credential> C getCredential(Class<C> credentialType) {
                  throw new UnsupportedOperationException();
               }

               @Override
               public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> evidenceType, String algorithmName) {
                  throw new UnsupportedOperationException();
               }

               @Override
               public boolean verifyEvidence(Evidence evidence) {
                  throw new UnsupportedOperationException();
               }

               @Override
               public boolean exists() {
                  return true;
               }
            };
         }
      };
   }

   /**
    * A builder for properties security realms.
    */
   public static class Builder {
      private String defaultRealm = null;
      private boolean plainText;
      private String groupsAttribute = "groups";

      Builder() {
      }

      /**
       * Where this realm returns an {@link AuthorizationIdentity} set the key on the Attributes that will be used to
       * hold the group membership information.
       *
       * @param groupsAttribute the key on the Attributes that will be used to hold the group membership information.
       * @return this {@link Builder}
       */
      public Builder setGroupsAttribute(final String groupsAttribute) {
         this.groupsAttribute = groupsAttribute;

         return this;
      }


      /**
       * Set the default realm name to use if no realm name is discovered in the properties file.
       *
       * @param defaultRealm the default realm name if one is not discovered in the properties file.
       * @return this {@link Builder}
       */
      public Builder setDefaultRealm(String defaultRealm) {
         this.defaultRealm = defaultRealm;

         return this;
      }

      /**
       * Set format of users property file - if the passwords are stored in plain text. Otherwise is HEX( MD5( username
       * ":" realm ":" password ) ) expected.
       *
       * @param plainText if the passwords are stored in plain text.
       * @return this {@link Builder}
       */
      public Builder setPlainText(boolean plainText) {
         this.plainText = plainText;

         return this;
      }

      /**
       * Builds the {@link EncryptedPropertiesSecurityRealm}.
       *
       * @return built {@link EncryptedPropertiesSecurityRealm}
       */
      public EncryptedPropertiesSecurityRealm build() {
         return new EncryptedPropertiesSecurityRealm(this);
      }

   }

   private static class LoadedState {

      private final Map<String, AccountEntry> accounts;
      private final String realmName;
      private final long loadTime;

      private LoadedState(Map<String, AccountEntry> accounts, String realmName, long loadTime) {
         this.accounts = accounts;
         this.realmName = realmName;
         this.loadTime = loadTime;
      }

      public Map<String, AccountEntry> getAccounts() {
         return accounts;
      }

      public String getRealmName() {
         return realmName;
      }

      public long getLoadTime() {
         return loadTime;
      }

   }

   private static class AccountEntry {

      private final String name;
      private final List<Credential> credentials;
      private final Set<String> groups;

      private AccountEntry(String name, List<Credential> credentials, String groups) {
         this.name = name;
         this.credentials = credentials;
         this.groups = convertGroups(groups);
      }

      private Set<String> convertGroups(String groups) {
         if (groups == null) {
            return Collections.emptySet();
         }

         return Arrays.stream(groups.split(","))
               .map(String::trim)
               .filter(s -> !s.isEmpty())
               .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
      }

      public String getName() {
         return name;
      }

      public List<Credential> getCredentials() {
         return credentials;
      }


      public Set<String> getGroups() {
         return groups;
      }
   }
}
