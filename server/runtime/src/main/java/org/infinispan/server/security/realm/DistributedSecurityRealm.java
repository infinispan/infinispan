package org.infinispan.server.security.realm;

import java.security.Principal;
import java.security.spec.AlgorithmParameterSpec;

import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.auth.server.SecurityRealm;
import org.wildfly.security.authz.AuthorizationIdentity;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.evidence.AlgorithmEvidence;
import org.wildfly.security.evidence.Evidence;

/**
 * A realm for authentication and authorization of identities distributed between multiple realms.
 *
 * @author <a href="mailto:mmazanek@redhat.com">Martin Mazanek</a>
 */
public class DistributedSecurityRealm implements SecurityRealm {
   private final SecurityRealm[] securityRealms;

   public DistributedSecurityRealm(final SecurityRealm... securityRealms) {
      this.securityRealms = securityRealms;
   }

   @Override
   public RealmIdentity getRealmIdentity(final Evidence evidence) throws RealmUnavailableException {
      return new EvidenceDistributedIdentity(evidence);
   }

   @Override
   public RealmIdentity getRealmIdentity(final Principal principal) throws RealmUnavailableException {
      return new PrincipalDistributedIdentity(principal);
   }

   @Override
   public SupportLevel getCredentialAcquireSupport(final Class<? extends Credential> credentialType, final String algorithmName, final AlgorithmParameterSpec parameterSpec) throws RealmUnavailableException {
      SupportLevel max = SupportLevel.UNSUPPORTED;
      for (SecurityRealm r : securityRealms) {
         max = SupportLevel.max(max, r.getCredentialAcquireSupport(credentialType, algorithmName, parameterSpec));
      }
      return max;
   }

   @Override
   public SupportLevel getEvidenceVerifySupport(final Class<? extends Evidence> evidenceType, final String algorithmName) throws RealmUnavailableException {
      SupportLevel max = SupportLevel.UNSUPPORTED;
      for (SecurityRealm r : securityRealms) {
         max = SupportLevel.max(max, r.getEvidenceVerifySupport(evidenceType, algorithmName));
      }
      return max;
   }

   final class EvidenceDistributedIdentity implements RealmIdentity {
      private final Evidence evidence;
      private final String evidenceAlgorithm;
      private RealmIdentity currentIdentity = RealmIdentity.NON_EXISTENT;
      private int nextRealm = 0;

      private EvidenceDistributedIdentity(Evidence evidence) throws RealmUnavailableException {
         this.evidence = evidence;
         if (evidence instanceof AlgorithmEvidence) {
            evidenceAlgorithm = ((AlgorithmEvidence) evidence).getAlgorithm();
         } else {
            evidenceAlgorithm = null;
         }
         nextIdentity();
      }

      private boolean nextIdentity() throws RealmUnavailableException {
         currentIdentity.dispose();
         if (nextRealm >= securityRealms.length) {
            currentIdentity = RealmIdentity.NON_EXISTENT;
            return false;
         }
         if (securityRealms[nextRealm].getEvidenceVerifySupport(evidence.getClass(), evidenceAlgorithm).mayBeSupported()) {
            currentIdentity = securityRealms[nextRealm].getRealmIdentity(evidence);
            nextRealm++;
            if (currentIdentity.getEvidenceVerifySupport(evidence.getClass(), evidenceAlgorithm).isNotSupported()) {
               return nextIdentity();
            }
         } else {
            nextRealm++;
            return nextIdentity();
         }
         return true;
      }

      @Override
      public Principal getRealmIdentityPrincipal() {
         return currentIdentity.getRealmIdentityPrincipal();
      }

      @Override
      public SupportLevel getCredentialAcquireSupport(final Class<? extends Credential> credentialType, final String algorithmName, final AlgorithmParameterSpec parameterSpec) throws RealmUnavailableException {
         // Identity created from evidence will be verified using the evidence
         return SupportLevel.UNSUPPORTED;
      }

      @Override
      public SupportLevel getEvidenceVerifySupport(final Class<? extends Evidence> evidenceType, final String algorithmName) throws RealmUnavailableException {
         //as server verifies evidence with same evidence used for creating realm identity, we dont have to look into remaining realms for support (currentIdentity will always support evidence verification, unless none of the possible does)
         return currentIdentity.getEvidenceVerifySupport(evidenceType, algorithmName);
      }

      @Override
      public <C extends Credential> C getCredential(final Class<C> credentialType, final String algorithmName, final AlgorithmParameterSpec parameterSpec) throws RealmUnavailableException {
         return null;
      }

      @Override
      public <C extends Credential> C getCredential(final Class<C> credentialType, final String algorithmName) throws RealmUnavailableException {
         return null;
      }

      @Override
      public <C extends Credential> C getCredential(final Class<C> credentialType) throws RealmUnavailableException {
         return null;
      }

      @Override
      public boolean verifyEvidence(final Evidence evidence) throws RealmUnavailableException {
         do {
            if (currentIdentity.verifyEvidence(evidence)) {
               return true;
            }
         } while (nextIdentity());
         return false;
      }

      @Override
      public boolean exists() throws RealmUnavailableException {
         return currentIdentity.exists();
      }

      @Override
      public AuthorizationIdentity getAuthorizationIdentity() throws RealmUnavailableException {
         return currentIdentity.getAuthorizationIdentity();
      }

      @Override
      public void dispose() {
         currentIdentity.dispose();
      }
   }

   final class PrincipalDistributedIdentity implements RealmIdentity {

      private final Principal principal;
      private RealmIdentity currentIdentity = RealmIdentity.NON_EXISTENT;
      private int nextRealm = 0;

      PrincipalDistributedIdentity(Principal principal) throws RealmUnavailableException {
         this.principal = principal;
         nextIdentity();
      }

      private boolean nextIdentity() throws RealmUnavailableException {
         currentIdentity.dispose();
         if (nextRealm >= securityRealms.length) {
            currentIdentity = RealmIdentity.NON_EXISTENT;
            return false;
         }
         currentIdentity = securityRealms[nextRealm].getRealmIdentity(principal);
         nextRealm++;
         if (!currentIdentity.exists()) {
            return nextIdentity();
         }
         return true;
      }

      @Override
      public Principal getRealmIdentityPrincipal() {
         return currentIdentity.getRealmIdentityPrincipal();
      }

      @Override
      public SupportLevel getCredentialAcquireSupport(final Class<? extends Credential> credentialType, final String algorithmName, final AlgorithmParameterSpec parameterSpec) throws RealmUnavailableException {
         return currentIdentity.getCredentialAcquireSupport(credentialType, algorithmName, parameterSpec);
      }

      @Override
      public SupportLevel getEvidenceVerifySupport(final Class<? extends Evidence> evidenceType, final String algorithmName) throws RealmUnavailableException {
         return currentIdentity.getEvidenceVerifySupport(evidenceType, algorithmName);
      }

      @Override
      public <C extends Credential> C getCredential(final Class<C> credentialType, final String algorithmName, final AlgorithmParameterSpec parameterSpec) throws RealmUnavailableException {
         do {
            System.out.printf("Identity= %s credentialType=%s algorithmName=%s parameterSpec=%s", currentIdentity.getRealmIdentityPrincipal(), credentialType, algorithmName, parameterSpec);
            C credential = currentIdentity.getCredential(credentialType, algorithmName, parameterSpec);
            if (credential != null) {
               return credential;
            }
         } while (nextIdentity());

         return null;
      }

      @Override
      public <C extends Credential> C getCredential(final Class<C> credentialType, final String algorithmName) throws RealmUnavailableException {
         do {
            System.out.printf("Identity= %s credentialType=%s algorithmName=%s", currentIdentity.getRealmIdentityPrincipal(), credentialType, algorithmName);
            C credential = currentIdentity.getCredential(credentialType, algorithmName);
            if (credential != null) {
               return credential;
            }
         } while (nextIdentity());

         return null;
      }

      @Override
      public <C extends Credential> C getCredential(final Class<C> credentialType) throws RealmUnavailableException {
         do {
            System.out.printf("Identity= %s credentialType=%s", currentIdentity.getRealmIdentityPrincipal(), credentialType);
            C credential = currentIdentity.getCredential(credentialType);
            if (credential != null) {
               return credential;
            }
         } while (nextIdentity());

         return null;
      }

      @Override
      public boolean verifyEvidence(final Evidence evidence) throws RealmUnavailableException {
         return currentIdentity.verifyEvidence(evidence);
      }

      @Override
      public boolean exists() throws RealmUnavailableException {
         return currentIdentity.exists();
      }

      @Override
      public AuthorizationIdentity getAuthorizationIdentity() throws RealmUnavailableException {
         return currentIdentity.getAuthorizationIdentity();
      }

      @Override
      public void dispose() {
         currentIdentity.dispose();
      }
   }

}
