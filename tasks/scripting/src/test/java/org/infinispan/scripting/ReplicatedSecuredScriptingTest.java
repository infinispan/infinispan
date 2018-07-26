package org.infinispan.scripting;

import static org.infinispan.scripting.utils.ScriptingUtils.getScriptingManager;
import static org.infinispan.scripting.utils.ScriptingUtils.loadScript;
import static org.testng.AssertJUnit.assertEquals;

import java.security.PrivilegedAction;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.tasks.TaskContext;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests verifying the script execution in secured clustered ispn environment.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "scripting.ReplicatedSecuredScriptingTest")
@CleanupAfterTest
public class ReplicatedSecuredScriptingTest extends MultipleCacheManagersTest {
    static final Subject ADMIN = TestingUtil.makeSubject("admin", ScriptingManager.SCRIPT_MANAGER_ROLE);
    static final Subject RUNNER = TestingUtil.makeSubject("runner", "runner");
    static final Subject PHEIDIPPIDES = TestingUtil.makeSubject("pheidippides", "pheidippides");

    @Override
    protected void createCacheManagers() throws Throwable {
        final GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
        final ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC);
        global.security().authorization().enable()
                .principalRoleMapper(new IdentityRoleMapper()).role("admin").permission(AuthorizationPermission.ALL)
                .role("runner")
                .permission(AuthorizationPermission.EXEC)
                .permission(AuthorizationPermission.READ)
                .permission(AuthorizationPermission.WRITE)
                .permission(AuthorizationPermission.ADMIN)
                .role("pheidippides")
                .permission(AuthorizationPermission.EXEC)
                .permission(AuthorizationPermission.READ)
                .permission(AuthorizationPermission.WRITE);
        builder.security().authorization().enable().role("admin").role("runner").role("pheidippides");
        builder.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE)
              .encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
        Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                createCluster(global, builder, 2);
                defineConfigurationOnAllManagers(SecureScriptingTest.SECURE_CACHE_NAME, builder);
                for (EmbeddedCacheManager cm : cacheManagers)
                    cm.getCache(SecureScriptingTest.SECURE_CACHE_NAME);
                waitForClusterToForm();
                return null;
            }
        });
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void destroy() {
        Security.doAs(ADMIN, new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                ReplicatedSecuredScriptingTest.super.destroy();
                return null;
            }
        });
    }

    @Override
    @AfterMethod(alwaysRun = true)
    protected void clearContent() throws Throwable {
        Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                try {
                    ReplicatedSecuredScriptingTest.super.clearContent();
                } catch (Throwable e) {
                    throw new Exception(e);
                }
                return null;
            }
        });
    }

    public void testLocalScriptExecutionWithRole() throws Exception {
        ScriptingManager scriptingManager = getScriptingManager(manager(0));

        Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                loadScript(scriptingManager, "/testRole.js");
                return null;
            }
        });

        Security.doAs(PHEIDIPPIDES, new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                Cache cache = manager(0).getCache(SecureScriptingTest.SECURE_CACHE_NAME);
                String value = (String) scriptingManager.runScript("testRole.js",
                        new TaskContext().cache(cache).addParameter("a", "value")).get();

                assertEquals("value", value);
                assertEquals("value", cache.get("a"));
                return null;
            }
        });
    }

    @Test(expectedExceptions = {PrivilegedActionException.class, SecurityException.class})
    public void testLocalScriptExecutionWithAuthException() throws Exception {
        ScriptingManager scriptingManager = getScriptingManager(manager(0));

        Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                loadScript(scriptingManager, "/testRole.js");
                return null;
            }
        });

        Security.doAs(RUNNER, new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                Cache cache = manager(0).getCache();
                scriptingManager.runScript("testRole.js",
                        new TaskContext().cache(cache).addParameter("a", "value")).get();
                return null;
            }
        });
    }

    @Test(enabled = false, description = "Enable when ISPN-6374 is fixed.")
    public void testDistributedScriptExecutionWithRole() throws Exception {
        ScriptingManager scriptingManager = getScriptingManager(manager(0));

        Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                loadScript(scriptingManager, "/testRole_dist.js");
                return null;
            }
        });

        Security.doAs(RUNNER, new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                Cache cache = manager(0).getCache();
                List<JGroupsAddress> value = (List<JGroupsAddress>) scriptingManager.runScript("testRole_dist.js",
                        new TaskContext().cache(cache).addParameter("a", "value")).get();

                assertEquals(value.get(0), manager(0).getAddress());
                assertEquals(value.get(1), manager(1).getAddress());
                assertEquals("value", cache.get("a"));
                assertEquals("value", manager(1).getCache().get("a"));
                return null;
            }
        });
    }

    @Test(expectedExceptions = {PrivilegedActionException.class, SecurityException.class})
    public void testDistributedScriptExecutionWithAuthException() throws Exception {
        ScriptingManager scriptingManager = getScriptingManager(manager(0));

        Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                loadScript(scriptingManager, "/testRole_dist.js");
                return null;
            }
        });

        Security.doAs(PHEIDIPPIDES, new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                Cache cache = manager(0).getCache();
                scriptingManager.runScript("testRole_dist.js",
                        new TaskContext().cache(cache).addParameter("a", "value")).get();
                return null;
            }
        });
    }

    @DataProvider(name = "cacheModeProvider")
    private static Object[][] providePrinciples() {
        return new Object[][] {{CacheMode.REPL_SYNC}, {CacheMode.DIST_SYNC}};
    }
}
