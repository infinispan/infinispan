package org.infinispan.server.server.security;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.spec.AlgorithmParameterSpec;

import org.wildfly.security.auth.SupportLevel;
import org.wildfly.security.auth.realm.LegacyPropertiesSecurityRealm;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.auth.server.SecurityRealm;
import org.wildfly.security.credential.Credential;
import org.wildfly.security.evidence.Evidence;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class PropertiesSecurityRealm implements SecurityRealm {
   private final File usersFile;
   private final File groupsFile;
   private final boolean plainText;
   private final String groupsAttribute;
   private final String realmName;
   LegacyPropertiesSecurityRealm delegate;

   public PropertiesSecurityRealm(File usersFile, File groupsFile, boolean plainText, String groupsAttribute, String realmName) {
      this.usersFile = usersFile;
      this.groupsFile = groupsFile;
      this.plainText = plainText;
      this.groupsAttribute = groupsAttribute;
      this.realmName = realmName;
   }

   private void load() throws IOException {
      try (InputStream usersInputStream = new FileInputStream(usersFile);
           InputStream groupsInputStream = groupsFile != null ? new FileInputStream(groupsFile) : null) {
         delegate = LegacyPropertiesSecurityRealm.builder()
               .setUsersStream(usersInputStream)
               .setGroupsStream(groupsInputStream)
               .setPlainText(plainText)
               .setGroupsAttribute(groupsAttribute)
               .setDefaultRealm(realmName)
               .build();
      }
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
