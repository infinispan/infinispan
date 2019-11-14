package org.infinispan.server.security.realm;

import java.security.spec.AlgorithmParameterSpec;

import org.wildfly.security.SecurityFactory;
import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.auth.server.SecurityRealm;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.credential.GSSKerberosCredential;
import org.wildfly.security.evidence.Evidence;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class KerberosSecurityRealm implements SecurityRealm {
   private final SecurityFactory<GSSKerberosCredential> kerberosCredentialSecurityFactory;

   public KerberosSecurityRealm(SecurityFactory<GSSKerberosCredential> kerberosCredentialSecurityFactory) {
      this.kerberosCredentialSecurityFactory = kerberosCredentialSecurityFactory;
   }

   @Override
   public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName, AlgorithmParameterSpec parameterSpec) throws RealmUnavailableException {
      return null;
   }

   @Override
   public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> evidenceType, String algorithmName) throws RealmUnavailableException {
      return null;
   }
}
