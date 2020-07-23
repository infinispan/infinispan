package org.infinispan.server.security.realm;

import static org.wildfly.common.Assert.checkNotNullParam;
import static org.infinispan.server.Server.log;

import java.security.Principal;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Arrays;

import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.realm.CacheableSecurityRealm;
import org.wildfly.security.auth.server.IdentityCredentials;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.auth.server.SecurityRealm;
import org.wildfly.security.auth.server.event.RealmEvent;
import org.wildfly.security.authz.Attributes;
import org.wildfly.security.authz.AuthorizationIdentity;
import org.wildfly.security.cache.RealmIdentityCache;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.evidence.Evidence;
import org.wildfly.security.evidence.PasswordGuessEvidence;
import org.wildfly.security.password.Password;
import org.wildfly.security.password.interfaces.ClearPassword;

/**
 * <p>A wrapper class that provides caching capabilities for a {@link SecurityRealm} and its identities.
 *
 * @author <a href="mailto:psilva@redhat.com">Pedro Igor</a>
 */
public class CachingSecurityRealm implements SecurityRealm {

   private final CacheableSecurityRealm realm;
   private final RealmIdentityCache cache;

   /**
    * Creates a new instance.
    *
    * @param realm the {@link SecurityRealm} whose {@link RealmIdentity} should be cached.
    * @param cache the {@link RealmIdentityCache} instance
    */
   public CachingSecurityRealm(CacheableSecurityRealm realm, RealmIdentityCache cache) {
      this.realm = checkNotNullParam("realm", realm);
      this.cache = checkNotNullParam("cache", cache);
      CacheableSecurityRealm cacheable = realm;
      cacheable.registerIdentityChangeListener(this::removeFromCache);
   }

   @Override
   public RealmIdentity getRealmIdentity(Principal principal) throws RealmUnavailableException {
      RealmIdentity cached = cache.get(principal);

      if (cached != null) {
         if(log.isTraceEnabled()) {
            log.tracef("Returning cached RealmIdentity for '%s'", principal);
         }
         return cached;
      }

      RealmIdentity realmIdentity = getCacheableRealm().getRealmIdentity(principal);

      if (!realmIdentity.exists()) {
         if(log.isTraceEnabled()) {
            log.tracef("RealmIdentity for '%s' does not exist, skipping cache.'", principal);
         }
         return realmIdentity;
      }

      RealmIdentity cachedIdentity = new RealmIdentity() {
         final RealmIdentity identity = realmIdentity;

         AuthorizationIdentity authorizationIdentity = null;
         Attributes attributes = null;
         IdentityCredentials credentials = IdentityCredentials.NONE;
         int verifiedEvidenceHash;

         @Override
         public Principal getRealmIdentityPrincipal() {
            return identity.getRealmIdentityPrincipal();
         }

         @Override
         public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName, final AlgorithmParameterSpec parameterSpec) throws RealmUnavailableException {
            if (credentials.contains(credentialType, algorithmName, parameterSpec)) {
               if (log.isTraceEnabled()) {
                  log.tracef("getCredentialAcquireSupport credentialType='%s' with algorithmName='%' known for pincipal='%s'", credentialType.getName(), algorithmName, principal.getName());
               }
               return credentials.getCredentialAcquireSupport(credentialType, algorithmName, parameterSpec);
            }
            Credential credential = identity.getCredential(credentialType, algorithmName, parameterSpec);
            if (credential != null) {
               if (log.isTraceEnabled()) {
                  log.tracef("getCredentialAcquireSupport Credential for credentialType='%s' with algorithmName='%' obtained from identity - caching for principal='%s'",
                        credentialType.getName(), algorithmName, principal.getName());
               }
               credentials = credentials.withCredential(credential);
            }
            return credentials.getCredentialAcquireSupport(credentialType, algorithmName, parameterSpec);
         }

         @Override
         public <C extends Credential> C getCredential(Class<C> credentialType) throws RealmUnavailableException {
            if (credentials.contains(credentialType)) {
               if (log.isTraceEnabled()) {
                  log.tracef("getCredential credentialType='%s' cached, returning cached credential for principal='%s'", credentialType.getName(), principal.getName());
               }
               return credentials.getCredential(credentialType);
            }
            Credential credential = identity.getCredential(credentialType);
            if (credential != null) {
               if (log.isTraceEnabled()) {
                  log.tracef("getCredential credentialType='%s' obtained from identity - caching for principal='%s'", credentialType.getName(), principal.getName());
               }
               credentials = credentials.withCredential(credential);
            }
            return credentials.getCredential(credentialType);
         }

         @Override
         public <C extends Credential> C getCredential(Class<C> credentialType, String algorithmName) throws RealmUnavailableException {
            if (credentials.contains(credentialType, algorithmName)) {
               if (log.isTraceEnabled()) {
                  log.tracef("getCredential credentialType='%s' with algorithmName='%' cached, returning cached credential for principal='%s'", credentialType.getName(), algorithmName, principal.getName());
               }
               return credentials.getCredential(credentialType, algorithmName);
            }
            Credential credential = identity.getCredential(credentialType, algorithmName);
            if (credential != null) {
               if (log.isTraceEnabled()) {
                  log.tracef("getCredential credentialType='%s' with algorithmName='%' obtained from identity - caching.", credentialType.getName(), algorithmName);
               }
               credentials = credentials.withCredential(credential);
            }
            return credentials.getCredential(credentialType, algorithmName);
         }

         @Override
         public <C extends Credential> C getCredential(final Class<C> credentialType, final String algorithmName, final AlgorithmParameterSpec parameterSpec) throws RealmUnavailableException {
            if (credentials.contains(credentialType, algorithmName, parameterSpec)) {
               if (log.isTraceEnabled()) {
                  log.tracef("getCredential credentialType='%s' with algorithmName='%s' cached, returning cached credential for principal='%s'", credentialType.getName(), algorithmName, principal.getName());
               }
               return credentials.getCredential(credentialType, algorithmName, parameterSpec);
            }
            Credential credential = identity.getCredential(credentialType, algorithmName, parameterSpec);
            if (credential != null) {
               if (log.isTraceEnabled()) {
                  log.tracef("getCredential credentialType='%s' with algorithmName='%s' obtained from identity - caching for principal='%s'", credentialType.getName(), algorithmName, principal.getName());
               }
               credentials = credentials.withCredential(credential);
            }
            return credentials.getCredential(credentialType, algorithmName, parameterSpec);
         }

         @Override
         public void updateCredential(Credential credential) throws RealmUnavailableException {
            if(log.isTraceEnabled()) {
               log.tracef("updateCredential For principal='%s'", principal);
            }
            try {
               identity.updateCredential(credential);
            } finally {
               removeFromCache(identity.getRealmIdentityPrincipal());
            }
         }

         @Override
         public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> evidenceType, String algorithmName) throws RealmUnavailableException {
            if (PasswordGuessEvidence.class.isAssignableFrom(evidenceType)) {
               if (credentials.canVerify(evidenceType, algorithmName)) {
                  if (log.isTraceEnabled()) {
                     log.tracef("getEvidenceVerifySupport evidenceType='%s' with algorithmName='%' can verify from cache for principal='%s'", evidenceType.getName(), algorithmName, principal.getName());
                  }
                  return SupportLevel.SUPPORTED;
               }
               Credential credential = identity.getCredential(PasswordCredential.class);
               if (credential != null) {
                  if (log.isTraceEnabled()) {
                     log.tracef("getEvidenceVerifySupport evidenceType='%s' with algorithmName='%' credential obtained from identity and cached for principal='%s'",
                           evidenceType.getName(), algorithmName, principal.getName());
                  }
                  credentials = credentials.withCredential(credential);
                  if (credential.canVerify(evidenceType, algorithmName)) {
                     return SupportLevel.SUPPORTED;
                  }
               }
            }
            if (log.isTraceEnabled()) {
               log.tracef("getEvidenceVerifySupport evidenceType='%s' with algorithmName='%' falling back to direct support of identity for principal='%s'",
                     evidenceType.getName(), algorithmName, principal.getName());
            }
            return identity.getEvidenceVerifySupport(evidenceType, algorithmName);
         }

         @Override
         public boolean verifyEvidence(Evidence evidence) throws RealmUnavailableException {
            if (evidence instanceof PasswordGuessEvidence) {
               char[] guess = ((PasswordGuessEvidence) evidence).getGuess();
               int evidenceHash = Arrays.hashCode(guess);
               if (evidenceHash == verifiedEvidenceHash) {
                  return true;
               }
               if (credentials.canVerify(evidence)) {
                  if(log.isTraceEnabled()) {
                     log.tracef("verifyEvidence For principal='%s' using cached credential", principal);
                  }
                  return credentials.verify(evidence);
               }
               Credential credential = identity.getCredential(PasswordCredential.class);
               if (credential != null) {
                  if(log.isTraceEnabled()) {
                     log.tracef("verifyEvidence Credential obtained from identity and cached for principal='%s'", principal);
                  }
                  credentials = credentials.withCredential(credential);
                  if (credential.canVerify(evidence)) {
                     boolean res = credential.verify(evidence);
                     if (res) {
                        verifiedEvidenceHash = evidenceHash;
                     }
                     return res;
                  }
               }
               Password password = ClearPassword.createRaw(ClearPassword.ALGORITHM_CLEAR, guess);
               if(log.isTraceEnabled()) {
                  log.tracef("verifyEvidence Falling back to direct support of identity for principal='%s'", principal);
               }
               if (identity.verifyEvidence(evidence)) {
                  credentials = credentials.withCredential(new PasswordCredential(password));
                  return true;
               }
               return false;
            }
            return identity.verifyEvidence(evidence);
         }

         @Override
         public boolean exists() throws RealmUnavailableException {
            return true; // non-existing identities will not be wrapped
         }

         @Override
         public AuthorizationIdentity getAuthorizationIdentity() throws RealmUnavailableException {
            if (authorizationIdentity == null) {
               if(log.isTraceEnabled()) {
                  log.tracef("getAuthorizationIdentity Caching AuthorizationIdentity for principal='%s'", principal);
               }
               authorizationIdentity = identity.getAuthorizationIdentity();
            }
            return authorizationIdentity;
         }

         @Override
         public Attributes getAttributes() throws RealmUnavailableException {
            if (attributes == null) {
               if(log.isTraceEnabled()) {
                  log.tracef("getAttributes Caching Attributes for principal='%s'", principal);
               }
               attributes = identity.getAttributes();
            }
            return attributes;
         }

         @Override
         public void dispose() {
            identity.dispose();
         }
      };

      if(log.isTraceEnabled()) {
         log.tracef("Created wrapper RealmIdentity for '%s' and placing in cache.", principal);
      }
      cache.put(principal, cachedIdentity);

      return cachedIdentity;
   }

   @Override
   public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName, final AlgorithmParameterSpec parameterSpec) throws RealmUnavailableException {
      return getCacheableRealm().getCredentialAcquireSupport(credentialType, algorithmName, parameterSpec);
   }

   @Override
   public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> evidenceType, String algorithmName) throws RealmUnavailableException {
      return getCacheableRealm().getEvidenceVerifySupport(evidenceType, algorithmName);
   }

   @Override
   public void handleRealmEvent(RealmEvent event) {
      getCacheableRealm().handleRealmEvent(event);
   }

   /**
    * Removes a {@link RealmIdentity} referenced by the specified {@link Principal} from the cache.
    *
    * @param principal the {@link Principal} that references a previously cached realm identity
    */
   public void removeFromCache(Principal principal) {
      cache.remove(principal);
   }

   /**
    * Removes all cached identities from the cache.
    */
   public void removeAllFromCache() {
      cache.clear();
   }

   /**
    * Gets wrapped backing realm.
    *
    * @return the wrapped backing realm
    */
   protected CacheableSecurityRealm getCacheableRealm() {
      return realm;
   }
}
