package org.infinispan.server.security.realm;

import java.security.Principal;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Collection;
import java.util.function.Function;

import org.wildfly.common.function.ExceptionConsumer;
import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.realm.CacheableSecurityRealm;
import org.wildfly.security.auth.server.ModifiableRealmIdentity;
import org.wildfly.security.auth.server.ModifiableRealmIdentityIterator;
import org.wildfly.security.auth.server.ModifiableSecurityRealm;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.auth.server.SecurityRealm;
import org.wildfly.security.authz.Attributes;
import org.wildfly.security.authz.AuthorizationIdentity;
import org.wildfly.security.cache.RealmIdentityCache;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.evidence.Evidence;

/**
 * <p>A wrapper class that provides caching capabilities for a {@link ModifiableSecurityRealm} and its identities.
 *
 * @author <a href="mailto:psilva@redhat.com">Pedro Igor</a>
 */
public class CachingModifiableSecurityRealm extends CachingSecurityRealm implements ModifiableSecurityRealm {

   /**
    * Creates a new instance.
    *
    * @param realm the {@link SecurityRealm} whose {@link RealmIdentity} should be cached..
    * @param cache the {@link RealmIdentityCache} instance
    */
   public CachingModifiableSecurityRealm(CacheableSecurityRealm realm, RealmIdentityCache cache) {
      super(realm, cache);
   }

   @Override
   public ModifiableRealmIdentity getRealmIdentityForUpdate(Principal principal) throws RealmUnavailableException {
      return wrap(getModifiableSecurityRealm().getRealmIdentityForUpdate(principal));
   }

   @Override
   public ModifiableRealmIdentityIterator getRealmIdentityIterator() throws RealmUnavailableException {
      ModifiableRealmIdentityIterator iterator = getModifiableSecurityRealm().getRealmIdentityIterator();
      return new ModifiableRealmIdentityIterator() {
         @Override
         public boolean hasNext() {
            return iterator.hasNext();
         }

         @Override
         public ModifiableRealmIdentity next() {
            return wrap(iterator.next());
         }
      };
   }

   private ModifiableRealmIdentity wrap(final ModifiableRealmIdentity modifiable) {
      return new ModifiableRealmIdentity() {
         @Override
         public void delete() throws RealmUnavailableException {
            executeAndInvalidate(modifiable -> {
               modifiable.delete();
            });
         }

         @Override
         public void create() throws RealmUnavailableException {
            modifiable.create();
         }

         @Override
         public void setCredentials(Collection<? extends Credential> credentials) throws RealmUnavailableException {
            executeAndInvalidate(modifiable -> {
               modifiable.setCredentials(credentials);
            });
         }

         @Override
         public void setAttributes(Attributes attributes) throws RealmUnavailableException {
            executeAndInvalidate(modifiable -> {
               modifiable.setAttributes(attributes);
            });
         }

         @Override
         public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName, final AlgorithmParameterSpec parameterSpec) throws RealmUnavailableException {
            return modifiable.getCredentialAcquireSupport(credentialType, algorithmName, parameterSpec);
         }

         @Override
         public <C extends Credential> C getCredential(Class<C> credentialType) throws RealmUnavailableException {
            return modifiable.getCredential(credentialType);
         }

         @Override
         public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> evidenceType, String algorithmName) throws RealmUnavailableException {
            return modifiable.getEvidenceVerifySupport(evidenceType, algorithmName);
         }

         @Override
         public boolean verifyEvidence(Evidence evidence) throws RealmUnavailableException {
            return modifiable.verifyEvidence(evidence);
         }

         @Override
         public boolean exists() throws RealmUnavailableException {
            return modifiable.exists();
         }

         @Override
         public void updateCredential(Credential credential) throws RealmUnavailableException {
            executeAndInvalidate(modifiable -> {
               modifiable.updateCredential(credential);
            });
         }

         @Override
         public Principal getRealmIdentityPrincipal() {
            return modifiable.getRealmIdentityPrincipal();
         }

         @Override
         public <C extends Credential> C getCredential(Class<C> credentialType, String algorithmName) throws RealmUnavailableException {
            return modifiable.getCredential(credentialType, algorithmName);
         }

         @Override
         public <C extends Credential> C getCredential(final Class<C> credentialType, final String algorithmName, final AlgorithmParameterSpec parameterSpec) throws RealmUnavailableException {
            return modifiable.getCredential(credentialType, algorithmName, parameterSpec);
         }

         @Override
         public <C extends Credential, R> R applyToCredential(Class<C> credentialType, Function<C, R> function) throws RealmUnavailableException {
            return modifiable.applyToCredential(credentialType, function);
         }

         @Override
         public <C extends Credential, R> R applyToCredential(Class<C> credentialType, String algorithmName, Function<C, R> function) throws RealmUnavailableException {
            return modifiable.applyToCredential(credentialType, algorithmName, function);
         }

         @Override
         public <C extends Credential, R> R applyToCredential(final Class<C> credentialType, final String algorithmName, final AlgorithmParameterSpec parameterSpec, final Function<C, R> function) throws RealmUnavailableException {
            return modifiable.applyToCredential(credentialType, algorithmName, parameterSpec, function);
         }

         @Override
         public void dispose() {
            modifiable.dispose();
         }

         @Override
         public AuthorizationIdentity getAuthorizationIdentity() throws RealmUnavailableException {
            return modifiable.getAuthorizationIdentity();
         }

         @Override
         public Attributes getAttributes() throws RealmUnavailableException {
            return modifiable.getAttributes();
         }

         private void executeAndInvalidate(ExceptionConsumer<ModifiableRealmIdentity, RealmUnavailableException> operation) throws RealmUnavailableException {
            try {
               operation.accept(modifiable);
            } catch (RealmUnavailableException rue) {
               throw rue;
            } finally {
               removeFromCache(modifiable.getRealmIdentityPrincipal());
            }
         }
      };
   }

   private ModifiableSecurityRealm getModifiableSecurityRealm() {
      return (ModifiableSecurityRealm) getCacheableRealm();
   }
}
