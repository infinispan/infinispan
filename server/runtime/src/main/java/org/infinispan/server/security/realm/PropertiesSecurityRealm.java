package org.infinispan.server.security.realm;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.Principal;
import java.security.spec.AlgorithmParameterSpec;
import java.util.Properties;
import java.util.function.Consumer;

import org.infinispan.server.Server;
import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.realm.CacheableSecurityRealm;
import org.wildfly.security.auth.server.RealmIdentity;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.evidence.Evidence;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class PropertiesSecurityRealm implements CacheableSecurityRealm {
   private final File usersFile;
   private final File groupsFile;
   private final boolean plainText;
   private final String groupsAttribute;
   private final String realmName;
   EncryptedPropertiesSecurityRealm delegate;

   public PropertiesSecurityRealm(File usersFile, File groupsFile, boolean plainText, String groupsAttribute, String realmName) {
      this.usersFile = usersFile;
      this.groupsFile = groupsFile;
      this.plainText = plainText;
      this.groupsAttribute = groupsAttribute;
      this.realmName = realmName;
      load();
   }

   private void load() {
      delegate = EncryptedPropertiesSecurityRealm.builder()
            .setPlainText(plainText)
            .setGroupsAttribute(groupsAttribute)
            .setDefaultRealm(realmName)
            .build();
   }

   void reload() {
      long loadTime = delegate.getLoadTime();
      if (usersFile.lastModified() > loadTime || groupsFile.lastModified() > loadTime) {
         try (InputStream usersInputStream = new FileInputStream(usersFile);
              InputStream groupsInputStream = groupsFile != null ? new FileInputStream(groupsFile) : null) {
            delegate.load(usersInputStream, groupsInputStream);
         } catch (IOException e) {
            throw Server.log.unableToLoadRealmPropertyFiles(e);
         }
      }
   }

   @Override
   public RealmIdentity getRealmIdentity(Principal principal) throws RealmUnavailableException {
      reload();
      return delegate.getRealmIdentity(principal);
   }

   @Override
   public RealmIdentity getRealmIdentity(Evidence evidence) throws RealmUnavailableException {
      reload();
      return delegate.getRealmIdentity(evidence);
   }

   @Override
   public SupportLevel getCredentialAcquireSupport(Class<? extends Credential> credentialType, String algorithmName, AlgorithmParameterSpec parameterSpec) {
      return delegate.getCredentialAcquireSupport(credentialType, algorithmName, parameterSpec);
   }

   @Override
   public SupportLevel getEvidenceVerifySupport(Class<? extends Evidence> evidenceType, String algorithmName) {
      return delegate.getEvidenceVerifySupport(evidenceType, algorithmName);
   }

   public boolean isEmpty() {
      Properties p = new Properties();
      try (InputStream is = new FileInputStream(usersFile)) {
         p.load(is);
      } catch (IOException e) {
         // Ignore
      }
      return p.isEmpty();
   }

   @Override
   public void registerIdentityChangeListener(Consumer<Principal> listener) {
      delegate.registerIdentityChangeListener(listener);
   }
}
