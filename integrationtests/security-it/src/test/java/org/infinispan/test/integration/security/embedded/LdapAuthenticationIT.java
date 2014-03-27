package org.infinispan.test.integration.security.embedded;

import static org.junit.Assert.assertEquals;

import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.infinispan.Cache;
import org.infinispan.security.AuthorizationPermission;
import org.jboss.arquillian.junit.Arquillian;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author vjuranek
 * @since 7.0
 */
@RunWith(Arquillian.class)
public class LdapAuthenticationIT extends AbstractLdapAuthentication {

   public static final String ADMIN_ROLE = "admin";
   public static final String ADMIN_PASSWD = "strong_password";
   public static final String WRITER_ROLE = "writer";
   public static final String WRITER_PASSWD = "some_password";
   public static final String READER_ROLE = "reader";
   public static final String READER_PASSWD = "password";
   public static final String UNPRIVILEGED_ROLE = "unprivileged";
   public static final String UNPRIVILEGED_PASSWD = "weak_password";

   public Map<String, AuthorizationPermission[]> getRolePermissionMap() {
      Map<String, AuthorizationPermission[]> roles = new HashMap<String, AuthorizationPermission[]>();
      roles.put(ADMIN_ROLE, new AuthorizationPermission[] { AuthorizationPermission.ALL });
      roles.put(WRITER_ROLE, new AuthorizationPermission[] { AuthorizationPermission.WRITE });
      roles.put(READER_ROLE, new AuthorizationPermission[] { AuthorizationPermission.READ });
      roles.put(UNPRIVILEGED_ROLE, new AuthorizationPermission[] { AuthorizationPermission.NONE });
      return roles;
   }

   public Subject getAdminSubject() throws LoginException {
      return authenticate(ADMIN_ROLE, ADMIN_PASSWD);
   }

   @Test
   public void testAdminCRUD() throws Exception {
      Subject admin = authenticate(ADMIN_ROLE, ADMIN_PASSWD);
      Subject.doAs(admin, new PrivilegedExceptionAction<Void>() {
         public Void run() throws Exception {
            assertEquals("predefined value", secureCache.get("predefined key"));
            secureCache.put("test", "test value");
            assertEquals("test value", secureCache.get("test"));
            Cache<Object, Object> c = manager.getCache("adminCache");
            c.start();
            c.put("test", "value");
            assertEquals("value", c.get("test"));
            c.remove("test");
            assertEquals(null, c.get("test"));
            c.stop();
            return null;
         }
      });
   }

   @Test
   public void testWriterWrite() throws Exception {
      Subject reader = authenticate(WRITER_ROLE, WRITER_PASSWD);
      Subject.doAs(reader, new PrivilegedExceptionAction<Void>() {
         public Void run() throws Exception {
            secureCache.put("test", "test value");
            return null;
         }
      });
   }

   @Test
   public void testWriterCreateWrite() throws Exception {
      Subject reader = authenticate(WRITER_ROLE, WRITER_PASSWD);
      Subject.doAs(reader, new PrivilegedExceptionAction<Void>() {
         public Void run() throws Exception {
            Cache<Object, Object> c = manager.getCache("writerCache");
            c.put("test", "value");
            return null;
         }
      });
   }

   @Test
   public void testWriterRemove() throws Exception {
      Subject reader = authenticate(WRITER_ROLE, WRITER_PASSWD);
      Subject.doAs(reader, new PrivilegedExceptionAction<Void>() {
         public Void run() throws Exception {
            secureCache.remove("predefined key");
            return null;
         }
      });
   }

   @Test(expected = java.lang.SecurityException.class)
   public void testWriterRead() throws Exception {
      Subject reader = authenticate(WRITER_ROLE, WRITER_PASSWD);
      Subject.doAs(reader, new PrivilegedExceptionAction<Void>() {
         public Void run() throws Exception {
            secureCache.get("predefined key");
            return null;
         }
      });
   }

   @Test
   public void testReaderRead() throws Exception {
      Subject reader = authenticate(READER_ROLE, READER_PASSWD);
      Subject.doAs(reader, new PrivilegedExceptionAction<Void>() {
         public Void run() throws Exception {
            assertEquals("predefined value", secureCache.get("predefined key"));
            return null;
         }
      });
   }

   @Test(expected = java.lang.SecurityException.class)
   public void testReaderWrite() throws Exception {
      Subject reader = authenticate(READER_ROLE, READER_PASSWD);
      Subject.doAs(reader, new PrivilegedExceptionAction<Void>() {
         public Void run() throws Exception {
            secureCache.put("test", "test value");
            return null;
         }
      });
   }

   @Test(expected = java.lang.SecurityException.class)
   public void testReaderRemove() throws Exception {
      Subject reader = authenticate(READER_ROLE, READER_PASSWD);
      Subject.doAs(reader, new PrivilegedExceptionAction<Void>() {
         public Void run() throws Exception {
            secureCache.remove("predefined key");
            return null;
         }
      });
   }

   @Test(expected = java.lang.SecurityException.class)
   public void testUnprivilegedRead() throws Exception {
      Subject reader = authenticate(UNPRIVILEGED_ROLE, UNPRIVILEGED_PASSWD);
      Subject.doAs(reader, new PrivilegedExceptionAction<Void>() {
         public Void run() throws Exception {
            secureCache.get("predefined key");
            return null;
         }
      });
   }

   @Test(expected = java.lang.SecurityException.class)
   public void testUnprivilegedWrite() throws Exception {
      Subject reader = authenticate(UNPRIVILEGED_ROLE, UNPRIVILEGED_PASSWD);
      Subject.doAs(reader, new PrivilegedExceptionAction<Void>() {
         public Void run() throws Exception {
            secureCache.put("test", "test value");
            return null;
         }
      });
   }

   @Test(expected = java.lang.SecurityException.class)
   public void testUnprivilegedRemove() throws Exception {
      Subject reader = authenticate(UNPRIVILEGED_ROLE, UNPRIVILEGED_PASSWD);
      Subject.doAs(reader, new PrivilegedExceptionAction<Void>() {
         public Void run() throws Exception {
            secureCache.remove("predefined key");
            return null;
         }
      });
   }

   @Test(expected = java.lang.SecurityException.class)
   public void testUnauthenticatedRead() throws Exception {
      secureCache.get("predefined key");
   }

   @Test(expected = java.lang.SecurityException.class)
   public void testUnauthenticatedWrite() throws Exception {
      secureCache.put("test", "value");
   }

   @Test(expected = java.lang.SecurityException.class)
   public void testUnauthenticatedRemove() throws Exception {
      secureCache.remove("predefined key");
   }

}
