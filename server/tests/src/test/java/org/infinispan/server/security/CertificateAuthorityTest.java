package org.infinispan.server.security;


import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;

import org.infinispan.server.test.core.CertificateAuthority;
import org.junit.jupiter.api.Test;

public class CertificateAuthorityTest {

   @Test
   public void testCA() throws GeneralSecurityException, IOException {
      CertificateAuthority ca = new CertificateAuthority("DC=infinispan,DC=org");
      ca.getCertificate("server");
      ca.getCertificate("user1");
      Path tmp = Paths.get("/tmp");
      ca.exportCertificateWithKey("server", tmp, "secret".toCharArray(), CertificateAuthority.ExportType.PFX);
      ca.exportCertificateWithKey("server", tmp, "secret".toCharArray(), CertificateAuthority.ExportType.BCFKS);
      ca.exportCertificateWithKey("user1", tmp, "secret".toCharArray(), CertificateAuthority.ExportType.PEM);
      ca.exportCertificates(tmp.resolve("ca.pfx"), CertificateAuthority.ExportType.PFX, "secret".toCharArray(), "ca");
   }

}
