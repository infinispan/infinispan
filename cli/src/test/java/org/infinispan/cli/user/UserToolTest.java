package org.infinispan.cli.user;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.infinispan.commons.util.Util;
import org.junit.Before;
import org.junit.Test;
import org.wildfly.common.iteration.CodePointIterator;
import org.wildfly.security.credential.PasswordCredential;
import org.wildfly.security.evidence.PasswordGuessEvidence;
import org.wildfly.security.password.PasswordFactory;
import org.wildfly.security.password.WildFlyElytronPasswordProvider;
import org.wildfly.security.password.spec.BasicPasswordSpecEncoding;
import org.wildfly.security.password.spec.PasswordSpec;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class UserToolTest {
   private static String tmpDirectory;
   private static File serverDirectory;
   private static File confDirectory;

   @Before
   public void createTestDirectory() {
      tmpDirectory = tmpDirectory(UserToolTest.class);
      Util.recursiveFileRemove(tmpDirectory);
      serverDirectory = new File(tmpDirectory, UserTool.DEFAULT_SERVER_ROOT);
      confDirectory = new File(serverDirectory, "conf");
      confDirectory.mkdirs();
   }

   @Test
   public void testUserToolClearText() throws IOException {
      UserTool userTool = new UserTool(serverDirectory.getAbsolutePath());
      userTool.createUser("user", "password", UserTool.DEFAULT_REALM_NAME, UserTool.Encryption.CLEAR, Collections.singletonList("admin"), null);
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
      UserTool userTool = new UserTool(serverDirectory.getAbsolutePath());
      userTool.createUser("user", "password", UserTool.DEFAULT_REALM_NAME, UserTool.Encryption.ENCRYPTED, Collections.singletonList("admin"), null);
      Properties users = new Properties();
      users.load(new FileReader(new File(confDirectory, "users.properties")));
      assertEquals(1, users.size());
      assertPassword("password", users.getProperty("user"));
      Properties groups = new Properties();
      groups.load(new FileReader(new File(confDirectory, "groups.properties")));
      assertEquals(1, groups.size());
      assertEquals("admin", groups.getProperty("user"));
   }

   @Test
   public void userManipulation() throws Exception {
      UserTool userTool = new UserTool(serverDirectory.getAbsolutePath());
      userTool.createUser("user", "password", UserTool.DEFAULT_REALM_NAME, UserTool.Encryption.ENCRYPTED, Arrays.asList("admin", "other"), null);
      userTool.reload();
      assertEquals("{ username: \"user\", realm: \"default\", groups = [admin, other] }", userTool.describeUser("user"));
      assertEquals(Collections.singletonList("user"), userTool.listUsers());
      assertEquals(Arrays.asList("admin", "other"), userTool.listGroups());
      userTool.modifyUser("user", null, null, UserTool.Encryption.DEFAULT, Arrays.asList("admin", "other", "else"), null);
      assertEquals("{ username: \"user\", realm: \"default\", groups = [admin, other, else] }", userTool.describeUser("user"));
      assertEquals(Arrays.asList("admin", "else", "other"), userTool.listGroups());
      userTool.removeUser("user");
      assertTrue(userTool.listUsers().isEmpty());
      assertTrue(userTool.listGroups().isEmpty());
   }

   @Test
   public void reEncrypt() throws Exception {
      UserTool userTool = new UserTool(serverDirectory.getAbsolutePath());
      userTool.createUser("user1", "password1", UserTool.DEFAULT_REALM_NAME, UserTool.Encryption.CLEAR, Arrays.asList("admin", "other"), null);
      userTool.createUser("user2", "password2", UserTool.DEFAULT_REALM_NAME, UserTool.Encryption.CLEAR, Arrays.asList("yetanother", "something"), null);
      Properties users = new Properties();
      users.load(new FileReader(new File(confDirectory, "users.properties")));
      assertEquals(2, users.size());
      assertEquals("password1", users.getProperty("user1"));
      assertEquals("password2", users.getProperty("user2"));
      userTool.reload();
      userTool.encryptAll(null);
      userTool.reload();
      users = new Properties();
      users.load(new FileReader(new File(confDirectory, "users.properties")));
      assertEquals(2, users.size());
      assertPassword("password1", users.getProperty("user1"));
      assertPassword("password2", users.getProperty("user2"));
   }

   private void assertPassword(String clear, String encrypted) throws NoSuchAlgorithmException, InvalidKeySpecException {
      PasswordGuessEvidence evidence = new PasswordGuessEvidence(clear.toCharArray());
      String[] split = encrypted.split(";");
      for (int i = 0; i < split.length; i++) {
         int colon = split[i].indexOf(':');
         String algorithm = split[i].substring(0, colon);
         String encoded = split[i].substring(colon + 1);
         byte[] passwordBytes = CodePointIterator.ofChars(encoded.toCharArray()).base64Decode().drain();
         PasswordFactory passwordFactory = PasswordFactory.getInstance(algorithm, WildFlyElytronPasswordProvider.getInstance());
         PasswordSpec passwordSpec = BasicPasswordSpecEncoding.decode(passwordBytes);
         PasswordCredential credential = new PasswordCredential(passwordFactory.generatePassword(passwordSpec));
         assertTrue("Passwords don't match", credential.verify(UserTool.PROVIDERS, evidence));
      }
   }
}
