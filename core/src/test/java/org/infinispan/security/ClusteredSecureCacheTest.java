package org.infinispan.security;

import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "security.ClusteredSecureCacheTest")
public class ClusteredSecureCacheTest extends MultipleCacheManagersTest {
   static final Subject ADMIN;
   static final Map<AuthorizationPermission, Subject> SUBJECTS;

   static {
      // Initialize one subject per permission
      SUBJECTS = TestingUtil.makeAllSubjects();
      ADMIN = SUBJECTS.get(AuthorizationPermission.ALL);
   }

   public CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      final GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      final ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode());
      GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable()
            .principalRoleMapper(new IdentityRoleMapper());
      for (AuthorizationPermission perm : AuthorizationPermission.values()) {
         globalRoles.role(perm.toString()).permission(perm);
      }
      global.serialization().addAdvancedExternalizer(4321, new SecureConsumer.Externalizer());
      AuthorizationConfigurationBuilder authConfig = builder.security().authorization().enable();
      for (AuthorizationPermission perm : AuthorizationPermission.values()) {
         authConfig.role(perm.toString());
      }
      Security.doAs(ADMIN, (PrivilegedExceptionAction<Void>) () -> {
         createCluster(global, builder, 2);
         waitForClusterToForm();
         return null;
      });
   }

   @Override
   @AfterClass(alwaysRun = true)
   protected void destroy() {
      Security.doAs(ADMIN, (PrivilegedAction<Void>) () -> {
         ClusteredSecureCacheTest.super.destroy();
         return null;
      });
   }

   @Override
   @AfterMethod(alwaysRun = true)
   protected void clearContent() throws Throwable {
      Security.doAs(ADMIN, (PrivilegedExceptionAction<Void>) () -> {
         try {
            ClusteredSecureCacheTest.super.clearContent();
         } catch (Throwable e) {
            throw new Exception(e);
         }
         return null;
      });
   }

   public void testClusteredSecureCache() {
      Security.doAs(ADMIN, (PrivilegedAction<Void>) () -> {
         Cache<String, String> cache1 = cache(0);
         Cache<String, String> cache2 = cache(1);
         cache1.put("a", "a");
         cache2.put("b", "b");
         assertEquals("a", cache2.get("a"));
         assertEquals("b", cache1.get("b"));
         return null;
      });
   }

   public void testSecureClusteredExecutor() {
      ClusterExecutor executor = Security.doAs(SUBJECTS.get(AuthorizationPermission.EXEC), (PrivilegedAction<ClusterExecutor>) () -> manager(0).executor());
      for (final AuthorizationPermission perm : AuthorizationPermission.values()) {
         Subject subject = SUBJECTS.get(perm);
         Security.doAs(subject, () -> {
            executor.allNodeSubmission().submitConsumer(
                  new SecureConsumer(),
                  (a, v, t) -> {
                     if (t != null) {
                        throw new RuntimeException(t);
                     } else {
                        // Ensure the Subject returned by the consumer matches the one it was invoked with
                        for(Principal principal : v.getPrincipals()) {
                           subject.getPrincipals().stream().filter(p -> p.getName().equals(principal.getName())).findFirst().orElseThrow();
                        }
                     }
                  }
            );
         });
      }
   }

   static class SecureConsumer implements Function<EmbeddedCacheManager, Subject>, Serializable {
      @Override
      public Subject apply(EmbeddedCacheManager m) {
         return Security.getSubject();
      }

      public static class Externalizer extends AbstractExternalizer<SecureConsumer> {
         @Override
         public Set<Class<? extends SecureConsumer>> getTypeClasses() {
            return Collections.singleton(SecureConsumer.class);
         }

         @Override
         public void writeObject(ObjectOutput output, SecureConsumer task) throws IOException {
         }

         @Override
         public SecureConsumer readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            return new SecureConsumer();
         }
      }
   }
}
