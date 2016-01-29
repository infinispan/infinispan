package org.infinispan.scripting;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import java.io.InputStream;

/**
 * Abstract class providing all general methods.
 */
public abstract class AbstractScriptingTest extends SingleCacheManagerTest {

    protected ScriptingManager scriptingManager;

    @Override
    protected EmbeddedCacheManager createCacheManager() throws Exception {
        return TestCacheManagerFactory.createCacheManager();
    }

    protected abstract String[] getScripts();

    @Override
    protected void setup() throws Exception {
        super.setup();
        scriptingManager = cacheManager.getGlobalComponentRegistry().getComponent(ScriptingManager.class);
        for (String scriptName : getScripts()) {
            try (InputStream is = this.getClass().getResourceAsStream("/" + scriptName)) {
                String script = TestingUtil.loadFileAsString(is);
                scriptingManager.addScript(scriptName, script);
            }
        }
    }
}
