package org.infinispan.security;

import static org.testng.AssertJUnit.assertEquals;

import java.security.Principal;
import java.util.Map;
import java.util.function.Function;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContextInitializer;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoSchema;
import org.infinispan.protostream.annotations.ProtoSyntax;
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
      global.serialization().addContextInitializer(new ClustedSecureCacheSCIImpl());
      AuthorizationConfigurationBuilder authConfig = builder.security().authorization().enable();
      for (AuthorizationPermission perm : AuthorizationPermission.values()) {
         authConfig.role(perm.toString());
      }
      Security.doAs(ADMIN,() -> {
         createCluster(global, builder, 2);
         waitForClusterToForm();
      });
   }

   @Override
   @AfterClass(alwaysRun = true)
   protected void destroy() {
      Security.doAs(ADMIN, () -> ClusteredSecureCacheTest.super.destroy());
   }

   @Override
   @AfterMethod(alwaysRun = true)
   protected void clearContent() throws Throwable {
      Security.doAs(ADMIN, () -> {
         try {
            ClusteredSecureCacheTest.super.clearContent();
         } catch (Throwable e) {
            throw new RuntimeException(e);
         }
      });
   }

   public void testClusteredSecureCache() {
      Security.doAs(ADMIN, () -> {
         Cache<String, String> cache1 = cache(0);
         Cache<String, String> cache2 = cache(1);
         cache1.put("a", "a");
         cache2.put("b", "b");
         assertEquals("a", cache2.get("a"));
         assertEquals("b", cache1.get("b"));
      });
   }

   public void testSecureClusteredExecutor() {
      ClusterExecutor executor = Security.doAs(SUBJECTS.get(AuthorizationPermission.EXEC), () -> manager(0).executor());
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

   static class SecureConsumer implements Function<EmbeddedCacheManager, Subject> {
      @Override
      public Subject apply(EmbeddedCacheManager m) {
         return Security.getSubject();
      }

      @ProtoFactory
      static SecureConsumer create() {
         return new SecureConsumer();
      }
   }

   @ProtoSchema(
         includeClasses = SecureConsumer.class,
         schemaFileName = "test.core.security.proto",
         schemaFilePath = "proto/generated",
         schemaPackageName = "org.infinispan.test.core.security",
         service = false,
         syntax = ProtoSyntax.PROTO3
   )
   public interface ClustedSecureCacheSCI extends SerializationContextInitializer {
   }
}
