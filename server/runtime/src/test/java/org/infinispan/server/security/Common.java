package org.infinispan.server.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.security.AuthorizationPermission;
import org.wildfly.security.sasl.util.SaslMechanismInformation;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class Common {

   public static final Map<String, User> USER_MAP;

   public static final Collection<Object[]> SASL_MECHS;

   static {
      USER_MAP = new HashMap<>();
      USER_MAP.put("admin", new User("admin", "adminPassword", AuthorizationPermission.ALL.name()));
      USER_MAP.put("supervisor", new User("supervisorPassword", AuthorizationPermission.ALL_READ.name(), AuthorizationPermission.ALL_WRITE.name()));
      USER_MAP.put("reader", new User("reader", "readerPassword", AuthorizationPermission.ALL_READ.name()));
      USER_MAP.put("writer", new User("writer", "writerPassword", AuthorizationPermission.ALL_WRITE.name()));
      USER_MAP.put("unprivileged", new User("unprivileged","unprivilegedPassword", AuthorizationPermission.NONE.name()));
      USER_MAP.put("executor", new User("executor", "executorPassword", AuthorizationPermission.EXEC.name()));

      SASL_MECHS = new ArrayList<>();
      SASL_MECHS.add(new Object[] { "" });
      SASL_MECHS.add(new Object[] { SaslMechanismInformation.Names.PLAIN });
      SASL_MECHS.add(new Object[] { SaslMechanismInformation.Names.DIGEST_MD5 });
      SASL_MECHS.add(new Object[] { SaslMechanismInformation.Names.DIGEST_SHA_512 });
      SASL_MECHS.add(new Object[] { SaslMechanismInformation.Names.DIGEST_SHA_384 });
      SASL_MECHS.add(new Object[] { SaslMechanismInformation.Names.DIGEST_SHA_256 });
      SASL_MECHS.add(new Object[] { SaslMechanismInformation.Names.DIGEST_SHA });
      SASL_MECHS.add(new Object[] { SaslMechanismInformation.Names.SCRAM_SHA_512 });
      SASL_MECHS.add(new Object[] { SaslMechanismInformation.Names.SCRAM_SHA_384 });
      SASL_MECHS.add(new Object[] { SaslMechanismInformation.Names.SCRAM_SHA_256 });
      SASL_MECHS.add(new Object[] { SaslMechanismInformation.Names.SCRAM_SHA_1 });
   }

   public static class User {
      final String username;
      final char[] password;
      final Iterable<String> groups;

      public User(String username, String password, String... groups) {
         this.username = username;
         this.password = password.toCharArray();
         this.groups = Arrays.asList(groups);
      }
   }
}
