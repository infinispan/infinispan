package org.infinispan.scripting;

import static org.testng.AssertJUnit.assertEquals;

import java.io.InputStream;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import javax.security.auth.Subject;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.AuthorizationConfigurationBuilder;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalAuthorizationConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.impl.ScriptTask;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.impl.IdentityRoleMapper;
import org.infinispan.tasks.Task;
import org.infinispan.tasks.TaskContext;
import org.infinispan.tasks.TaskExecutionMode;
import org.infinispan.tasks.TaskManager;
import org.infinispan.tasks.spi.TaskEngine;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verifying the script execution over task management with secured cache.
 *
 * @author amanukya
 */
@Test(groups="functional", testName="scripting.SecureScriptingTaskManagerTest")
@CleanupAfterMethod
public class SecureScriptingTaskManagerTest extends SingleCacheManagerTest {

    protected static final String SCRIPT_NAME = "testRole.js";
    protected TaskManager taskManager;

    static final Subject ADMIN = TestingUtil.makeSubject("admin", ScriptingManager.SCRIPT_MANAGER_ROLE);
    static final Subject RUNNER = TestingUtil.makeSubject("runner", "runner");
    static final Subject PHEIDIPPIDES = TestingUtil.makeSubject("pheidippides", "pheidippides");

    @Override
    protected EmbeddedCacheManager createCacheManager() throws Exception {
        GlobalConfigurationBuilder global = new GlobalConfigurationBuilder();
        GlobalAuthorizationConfigurationBuilder globalRoles = global.security().authorization().enable().principalRoleMapper(new IdentityRoleMapper());
        ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
        AuthorizationConfigurationBuilder authConfig = config.security().authorization().enable();

        globalRoles
                .role("runner")
                .permission(AuthorizationPermission.EXEC)
                .permission(AuthorizationPermission.READ)
                .permission(AuthorizationPermission.WRITE)
                .role("pheidippides")
                .permission(AuthorizationPermission.EXEC)
                .permission(AuthorizationPermission.READ)
                .permission(AuthorizationPermission.WRITE)
                .role("admin")
                .permission(AuthorizationPermission.ALL);
        authConfig.role("runner").role("pheidippides").role("admin");
        return TestCacheManagerFactory.createCacheManager(global, config);
    }

    @Override
    protected void setup() throws Exception {
        Security.doAs(ADMIN, new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                SecureScriptingTaskManagerTest.super.setup();
                taskManager = cacheManager.getGlobalComponentRegistry().getComponent(TaskManager.class);
                Cache<String, String> scriptCache = cacheManager.getCache(ScriptingManager.SCRIPT_CACHE);
                try (InputStream is = this.getClass().getResourceAsStream("/testRole.js")) {
                    String script = TestingUtil.loadFileAsString(is);
                    scriptCache.put(SCRIPT_NAME, script);
                }
                cacheManager.defineConfiguration(SecureScriptingTest.SECURE_CACHE_NAME, cacheManager.getDefaultCacheConfiguration());
                cacheManager.getCache(SecureScriptingTest.SECURE_CACHE_NAME);
                return null;
            }
        });
    }

    @Override
    protected void teardown() {
        Security.doAs(ADMIN, new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                SecureScriptingTaskManagerTest.super.teardown();
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

    public void testTask() throws Exception {
        Security.doAs(PHEIDIPPIDES, new PrivilegedExceptionAction<Void>() {
            @Override
            public Void run() throws Exception {
                String result = null;
                result = (String) taskManager.runTask(SCRIPT_NAME, new TaskContext().addParameter("a", "a")).get();

                assertEquals("a", result);

                return null;
            }
        });
        List<Task> tasks = taskManager.getTasks();
        assertEquals(1, tasks.size());

        ScriptTask scriptTask = (ScriptTask) tasks.get(0);
        assertEquals(SCRIPT_NAME, scriptTask.getName());
        assertEquals(TaskExecutionMode.ONE_NODE, scriptTask.getExecutionMode());
        assertEquals("Script", scriptTask.getType());
    }

    public void testAvailableEngines() {
        List<TaskEngine> engines = taskManager.getEngines();
        assertEquals(1, engines.size());
        assertEquals("Script", engines.get(0).getName());
    }
}
