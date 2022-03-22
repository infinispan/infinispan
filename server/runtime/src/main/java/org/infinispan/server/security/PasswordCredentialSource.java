package org.infinispan.server.security;

import java.io.IOException;
import java.security.spec.AlgorithmParameterSpec;
import java.util.function.Supplier;

import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.credential.source.CredentialSource;
import org.wildfly.security.password.interfaces.ClearPassword;

/**
 * @since 14.0
 */
public class PasswordCredentialSource implements CredentialSource, Supplier<CredentialSource> {
   private final PasswordCredential credential;

   public PasswordCredentialSource(char[] password) {
      this(new PasswordCredential(ClearPassword.createRaw(ClearPassword.ALGORITHM_CLEAR, password)));
   }

   public PasswordCredentialSource(PasswordCredential credential) {
      this.credential = credential;
   }

   @Override
   public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String s, AlgorithmParameterSpec algorithmParameterSpec) {
      return credentialType == PasswordCredential.class ? SupportLevel.SUPPORTED : SupportLevel.UNSUPPORTED;
   }

   @Override
   public <C extends Credential> C getCredential(Class<C> credentialType, String s, AlgorithmParameterSpec algorithmParameterSpec) throws IOException {
      return credentialType.cast(credential.clone());
   }

   @Override
   public CredentialSource get() {
      return this;
   }
}
