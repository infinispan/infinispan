package org.infinispan.server.usertool;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Properties;

import org.infinispan.server.Server;
import org.infinispan.server.security.UserTool;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wildfly.common.iteration.CodePointIterator;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.evidence.PasswordGuessEvidence;
import org.wildfly.security.password.PasswordFactory;
import org.wildfly.security.password.spec.BasicPasswordSpecEncoding;
import org.wildfly.security.password.spec.PasswordSpec;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class UserToolTest {
   private static String tmpDirectory;
   private static File confDirectory;

   @BeforeClass
   public static void createTestDirectory() {
      tmpDirectory = tmpDirectory(UserToolTest.class);
      confDirectory = new File(tmpDirectory, Server.DEFAULT_SERVER_CONFIG);
      confDirectory.mkdirs();
   }

   @Test
   public void testUserToolClearText() throws IOException {
      UserTool userTool = new UserTool();
      userTool.run("-b", "-c", "-u", "user", "-p", "password", "-s", tmpDirectory, "-g", "admin");
      Properties users = new Properties();
      users.load(new FileReader(new File(confDirectory, "users.properties")));
      assertEquals(1, users.size());
      assertEquals("password", users.getProperty("user"));
      Properties groups = new Properties();
      groups.load(new FileReader(new File(confDirectory, "groups.properties")));
      assertEquals(1, groups.size());
      assertEquals("admin", groups.getProperty("user"));
   }

   @Test
   public void testUserToolEncrypted() throws Exception {
      UserTool userTool = new UserTool();
      userTool.run("-b", "-e", "-u", "user", "-p", "password", "-s", tmpDirectory, "-g", "admin");
      Properties users = new Properties();
      users.load(new FileReader(new File(confDirectory, "users.properties")));
      assertEquals(1, users.size());
      assertPassword("password", users.getProperty("user"));
      Properties groups = new Properties();
      groups.load(new FileReader(new File(confDirectory, "groups.properties")));
      assertEquals(1, groups.size());
      assertEquals("admin", groups.getProperty("user"));
   }

   private void assertPassword(String clear, String encrypted) throws NoSuchAlgorithmException, InvalidKeySpecException {
      PasswordGuessEvidence evidence = new PasswordGuessEvidence(clear.toCharArray());
      String[] split = encrypted.split(";");
      for (int i = 0; i < split.length; i++) {
         int colon = split[i].indexOf(':');
         String algorithm = split[i].substring(0, colon);
         String encoded = split[i].substring(colon + 1);
         byte[] passwordBytes = CodePointIterator.ofChars(encoded.toCharArray()).base64Decode().drain();
         PasswordFactory passwordFactory = PasswordFactory.getInstance(algorithm);
         PasswordSpec passwordSpec = BasicPasswordSpecEncoding.decode(passwordBytes);
         PasswordCredential credential = new PasswordCredential(passwordFactory.generatePassword(passwordSpec));
         assertTrue("Passwords don't match", credential.verify(evidence));
      }
   }
}
