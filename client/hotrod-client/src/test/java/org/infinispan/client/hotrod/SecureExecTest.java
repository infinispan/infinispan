package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.commons.equivalence.AnyServerEquivalence;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
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
    static final Subject ADMIN = TestingUtil.makeSubject("user", ScriptingManagerImpl.SCRIPT_MANAGER_ROLE);

    private RemoteCacheManager remoteCacheManager;

    @Override
    protected EmbeddedCacheManager createCacheManager() throws Exception {
        GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
        GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable().principalRoleMapper(new IdentityRoleMapper());
        globalRoles
                .role("user")
                .permission(AuthorizationPermission.ALL)
                .permission(AuthorizationPermission.EXEC);

        ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
        config.dataContainer().keyEquivalence(new AnyServerEquivalence())
                .valueEquivalence(new AnyServerEquivalence()).compatibility().enable()
                .marshaller(new GenericJBossMarshaller())
                .security().authorization().enable().role("user");
        cacheManager = TestCacheManagerFactory.createCacheManager(global, config);
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
        cacheManager.getCache().clear();
    }

    protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder initServerAndClient() {
        return Security.doAs(ADMIN, new PrivilegedAction<org.infinispan.client.hotrod.configuration.ConfigurationBuilder>() {
            @Override
            public org.infinispan.client.hotrod.configuration.ConfigurationBuilder run() {
                return SecureExecTest.super.initServerAndClient();
            }
        });
    }

    public void testSimpleScriptExecutionWithValidAuth() throws IOException, PrivilegedActionException {
        org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = initServerAndClient();
        clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("user", "realm", "password".toCharArray()));

        runTestWithGivenScript(clientBuilder.build(), "/testRole_hotrod.js");
    }

    @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*Unauthorized access.*")
    public void testSimpleScriptExecutionWithInValidAuth() throws IOException, PrivilegedActionException {
        org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder = initServerAndClient();
        clientBuilder.security().authentication().callbackHandler(new TestCallbackHandler("user", "realm", "password".toCharArray()));

        runTestWithGivenScript(clientBuilder.build(), "/testRole.js");
    }

    private void runTestWithGivenScript(Configuration config, String scriptPath) throws IOException, PrivilegedActionException {
        remoteCacheManager = new RemoteCacheManager(config);
        Map<String, String> params = new HashMap<>();
        params.put("a", "guinness");

        ScriptingManager scriptingManager = hotrodServer.getCacheManager().getGlobalComponentRegistry().getComponent(ScriptingManager.class);
        String scriptName = null;
        try (InputStream is = this.getClass().getResourceAsStream(scriptPath)) {
            String script = TestingUtil.loadFileAsString(is);

            scriptName = scriptPath.substring(1);
            scriptingManager.addScript(scriptName, script);
        }

        String result = remoteCacheManager.getCache().execute(scriptName, params);
        assertEquals("guinness", result);
        assertEquals("guinness", remoteCacheManager.getCache().get("a"));
    }

}
