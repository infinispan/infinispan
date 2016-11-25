package org.infinispan.xsite;

import static org.testng.AssertJUnit.assertEquals;

import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;

import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "xsite", testName = "xsite.BackupWithSecurityTest")
public class BackupWithSecurityTest extends AbstractMultipleSitesTest {
   static final Subject ADMIN;
   static final Map<AuthorizationPermission, Subject> SUBJECTS;
   public static final String XSITECACHE = "XSITECACHE";

   static {      // Initialize one subject per permission
      SUBJECTS = new HashMap<>(AuthorizationPermission.values().length);
      for (AuthorizationPermission perm : AuthorizationPermission.values()) {
         SUBJECTS.put(perm, TestingUtil.makeSubject(perm.toString() + "_user", perm.toString()));
      }
      ADMIN = SUBJECTS.get(AuthorizationPermission.ALL);
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      ConfigurationBuilder builder = super.defaultConfigurationForSite(siteIndex);
      AuthorizationConfigurationBuilder authConfig = builder.security().authorization().enable();

      for (AuthorizationPermission perm : AuthorizationPermission.values()) {
         authConfig.role(perm.toString());
      }
      return builder;
   }

   @Override
   protected GlobalConfigurationBuilder defaultGlobalConfigurationForSite(int siteIndex) {
      GlobalConfigurationBuilder builder = super.defaultGlobalConfigurationForSite(siteIndex);

      GlobalAuthorizationConfigurationBuilder globalRoles = builder.security().authorization().enable().principalRoleMapper(new IdentityRoleMapper());
      for (AuthorizationPermission perm : AuthorizationPermission.values()) {
         globalRoles.role(perm.toString()).permission(perm);
      }
      return builder;
   }

   @Override
   protected TestSite createSite(String siteName, int numNodes, GlobalConfigurationBuilder gcb, ConfigurationBuilder defaultCacheConfig) {
      return Security.doAs(ADMIN, (PrivilegedAction<TestSite>) () -> BackupWithSecurityTest.super.createSite(siteName, numNodes, gcb, defaultCacheConfig));
   }

   @Override
   protected void killSite(TestSite ts) {
      Security.doAs(ADMIN, (PrivilegedAction<Void>) () -> {
         BackupWithSecurityTest.super.killSite(ts);
         return null;
      });
   }

   @Override
   protected void clearSite(TestSite ts) {
      Security.doAs(ADMIN, (PrivilegedAction<Object>) () -> {
         BackupWithSecurityTest.super.clearSite(ts);
         return null;
      });
   }

   @Override
   protected void afterSitesCreated() {
      super.afterSitesCreated();
      Security.doAs(ADMIN, (PrivilegedAction<Void>) () -> {
         ConfigurationBuilder builder = defaultConfigurationForSite(0);
         builder.sites().addBackup().site(siteName(1)).strategy(BackupConfiguration.BackupStrategy.SYNC);
         defineInSite(site(0), XSITECACHE, builder.build());
         site(0).waitForClusterToForm(XSITECACHE);

         builder = defaultConfigurationForSite(1);
         defineInSite(site(1), XSITECACHE, builder.build());
         site(1).waitForClusterToForm(XSITECACHE);
         return null;
      });
   }

   public void testBackupCacheAccess() {
      Security.doAs(SUBJECTS.get(AuthorizationPermission.WRITE), (PrivilegedAction<Void>) () -> {
         site(0).cache(XSITECACHE, 0).put("k1", "v1");
         return null;
      });
      String v = Security.doAs(SUBJECTS.get(AuthorizationPermission.READ), (PrivilegedAction<String>) () -> (String) site(1).cache(XSITECACHE, 0).get("k1"));
      assertEquals("v1", v);

   }
}
