package org.infinispan.scripting;

import static org.infinispan.commons.test.CommonsTestingUtil.loadFileAsString;

import java.io.InputStream;

import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;

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
        scriptingManager = GlobalComponentRegistry.componentOf(cacheManager, ScriptingManager.class);
        for (String scriptName : getScripts()) {
            try (InputStream is = this.getClass().getResourceAsStream("/" + scriptName)) {
                String script = loadFileAsString(is);
                scriptingManager.addScript(scriptName, script);
            }
        }
    }
}
