package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.scripting.impl.ScriptingManagerImpl;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.server.hotrod.test.TestCallbackHandler;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.junit.Ignore;
import org.testng.annotations.Test;

import javax.security.auth.Subject;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Tests verifying script execution over HotRod Client with enabled authentication.
 *
 * @author Anna Manukyan
 */
@Test(testName = "client.hotrod.SecureExecTest", groups = "functional")
@CleanupAfterMethod
public class SecureExecTest extends AuthenticationTest {
    static final Subject ADMIN = TestingUtil.makeSubject("admin", ScriptingManagerImpl.SCRIPT_MANAGER_ROLE);
    static final Subject RUNNER = TestingUtil.makeSubject("runner", "runner");
    static final Subject PHEIDIPPIDES = TestingUtil.makeSubject("pheidippides", "pheidippides");

    private RemoteCacheManager remoteCacheManager;

    @Override
    protected EmbeddedCacheManager createCacheManager() throws Exception {
        GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
        GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable().principalRoleMapper(new IdentityRoleMapper());
        globalRoles
                .role("runner")
                .permission(AuthorizationPermission.EXEC)
                .permission(AuthorizationPermission.READ)
                .permission(AuthorizationPermission.WRITE)
                .permission(AuthorizationPermission.ADMIN)
                .role("pheidippides")
                .permission(AuthorizationPermission.READ)
                .permission(AuthorizationPermission.WRITE)
                .permission(AuthorizationPermission.ADMIN)
                .role("admin")
                .permission(AuthorizationPermission.ALL);

        ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
        config.security().authorization().enable().role("runner").role("pheidippides").role("admin");
        cacheManager = TestCacheManagerFactory.createCacheManager(global, hotRodCacheConfiguration());
        cacheManager.getCache();

        return cacheManager;
    }

    @Override
    protected void setup() throws Exception {
        Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                SecureExecTest.super.setup();
                return null;
            }
        });
    }

    @Override
    protected void teardown() {
        Security.doAs(ADMIN, new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                SecureExecTest.super.teardown();
                return null;
            }
        });
    }

    @Override
    protected void clearContent() {
        Security.doAs(ADMIN, new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                cacheManager.getCache().clear();
                return null;
            }
        });
    }

    protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder initServerAndClient() {
        return Security.doAs(ADMIN, new PrivilegedAction<org.infinispan.client.hotrod.configuration.ConfigurationBuilder>() {
            @Override
            public org.infinispan.client.hotrod.configuration.ConfigurationBuilder run() {
                return SecureExecTest.super.initServerAndClient();
            }
        });
    }

    @Test(enabled = false, description = "Disabled until issue ISPN-6210 is fixed.")
    public void testSimpleScriptExecutionWithValidAuth() throws IOException, PrivilegedActionException {
        org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = initServerAndClient();
        clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("user", "realm", "password".toCharArray()));

        runTestWithGivenConfig(clientBuilder.build(), RUNNER);
    }

    @Test(enabled = false, description = "Disabled until issue ISPN-6210 is fixed.")
    public void testSimpleScriptExecutionWithInValidAuth() throws IOException, PrivilegedActionException {
        org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = initServerAndClient();
        clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("user", "realm", "password".toCharArray()));

        runTestWithGivenConfig(clientBuilder.build(), PHEIDIPPIDES);
    }

    private void runTestWithGivenConfig(Configuration config, Subject subject) throws IOException, PrivilegedActionException {
        remoteCacheManager = new RemoteCacheManager(config);
        Map<String, String> params = new HashMap<>();
        params.put("a", "guinness");

        Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                ScriptingManager scriptingManager = hotrodServer.getCacheManager().getGlobalComponentRegistry().getComponent(ScriptingManager.class);

                try (InputStream is = this.getClass().getResourceAsStream("/testRole.js")) {
                    String script = TestingUtil.loadFileAsString(is);
                    scriptingManager.addScript("testRole.js", script);
                }

                return null;
            }
        });

        Security.doAs(subject, new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                Integer result = remoteCacheManager.getCache().execute("testRole.js", params);
                assertEquals("guinness", remoteCacheManager.getCache().get("a"));

                return null;
            }
        });
    }

}
