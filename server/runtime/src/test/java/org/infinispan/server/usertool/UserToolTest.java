package org.infinispan.server.usertool;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.infinispan.server.Server;
import org.infinispan.server.security.UserTool;
import org.infinispan.test.TestingUtil;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class UserToolTest {
   private static String tmpDirectory;
   private static File confDirectory;

   @BeforeClass
   public static void createTestDirectory() {
      tmpDirectory = TestingUtil.tmpDirectory(UserToolTest.class);
      confDirectory = new File(tmpDirectory, Server.DEFAULT_SERVER_CONFIG);
      confDirectory.mkdirs();
   }

   @Test
   public void testUserToolPlain() throws IOException {
      UserTool userTool = new UserTool();
      userTool.run("-b", "-u", "user", "-p", "password", "-s", tmpDirectory, "-g", "admin");
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
   public void testUserToolDigest() throws IOException {
      UserTool userTool = new UserTool();
      userTool.run("-b", "-d", "-u", "user", "-p", "password", "-s", tmpDirectory, "-g", "admin");
      Properties users = new Properties();
      users.load(new FileReader(new File(confDirectory, "users.properties")));
      assertEquals(1, users.size());
      assertEquals("98632ccf8d10d1ecc86bb1d1522b4dcf", users.getProperty("user"));
      Properties groups = new Properties();
      groups.load(new FileReader(new File(confDirectory, "groups.properties")));
      assertEquals(1, groups.size());
      assertEquals("admin", groups.getProperty("user"));
   }
}
