package org.infinispan.test.integration.security.embedded;

import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Map.Entry;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.global.GlobalRoleConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.test.integration.security.utils.ApacheDsLdap;
import org.infinispan.test.integration.security.utils.Deployments;
import org.infinispan.test.integration.security.utils.LoginHandler;
import org.infinispan.transaction.LockingMode;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.shrinkwrap.api.spec.WebArchive;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * @author vjuranek
 * @since 7.0
 */
public abstract class AbstractLdapAuthentication {

   public static final String SECURITY_DOMAIN_NAME = "ispn-secure";
   public static final String CACHE_NAME = "secureCache";

   private static ApacheDsLdap ldapServer;
   protected EmbeddedCacheManager manager;
   protected Cache<Object, Object> secureCache;
   GlobalConfigurationBuilder globalConfig;
   private ConfigurationBuilder cacheConfig;
   
   public abstract Map<String, AuthorizationPermission[]> getRolePermissionMap();
   public abstract Subject getAdminSubject() throws LoginException; 
   
   public Subject authenticate(String login, String password) throws LoginException {
      final String securityDomain = System.getProperty("jboss.security.domain", SECURITY_DOMAIN_NAME);
      LoginContext lc = new LoginContext(securityDomain, new LoginHandler(login, password));
      lc.login();
      return lc.getSubject();
   }

   @Before
   public void setupCache() throws Exception {
      //global setup
      globalConfig = new GlobalConfigurationBuilder();
      globalConfig.globalJmxStatistics().disable();
      globalConfig.globalJmxStatistics().mBeanServerLookup(null); //TODO remove once WFLY-3124 is fixed, for now fail JMX registration

      GlobalAuthorizationConfigurationBuilder globalRoles = globalConfig.security().authorization()
            .principalRoleMapper(new IdentityRoleMapper());

      //cache setup
      cacheConfig = new ConfigurationBuilder();
      cacheConfig.transaction().lockingMode(LockingMode.PESSIMISTIC);
      cacheConfig.invocationBatching().enable();
      cacheConfig.jmxStatistics().disable();
      AuthorizationConfigurationBuilder authConfig = cacheConfig.security().enable().authorization();

      //authorization setup
      Map<String, AuthorizationPermission[]> rolePermissionMap = getRolePermissionMap();
      for (Entry<String, AuthorizationPermission[]> role : rolePermissionMap.entrySet()) {
         authConfig = authConfig.role(role.getKey());
         GlobalRoleConfigurationBuilder roleBuilder = globalRoles.role(role.getKey());
         for (AuthorizationPermission permission : role.getValue()) {
            roleBuilder = roleBuilder.permission(permission);
         }
      }
      
      Subject admin = getAdminSubject();
      Subject.doAs(admin, new PrivilegedExceptionAction<Void>() {
         public Void run() throws Exception {
            manager = new DefaultCacheManager(globalConfig.build());
            manager.defineConfiguration(CACHE_NAME, cacheConfig.build());
            secureCache = manager.getCache(CACHE_NAME);
            secureCache.put("predefined key", "predefined value");
            return null;
         }
      });
   }

   @After
   public void tearDown() throws Exception {
      if (manager != null) {
         Subject admin = getAdminSubject();
         Subject.doAs(admin, new PrivilegedExceptionAction<Void>() {
            public Void run() throws Exception {
               manager.stop();
               return null;
            }
         });
      }
   }

   @BeforeClass
   public static void ldapSetup() throws Exception {
      ldapServer = new ApacheDsLdap("localhost");
      ldapServer.start();
   }

   @AfterClass
   public static void ldapTearDown() throws Exception {
      ldapServer.stop();
   }

   @Deployment
   public static WebArchive getDeployment() {
      return Deployments.createDeployment();
   }

}
