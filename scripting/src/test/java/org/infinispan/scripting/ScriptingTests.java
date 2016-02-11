package org.infinispan.scripting;

import org.infinispan.test.TestingUtil;

import java.io.IOException;
import java.io.InputStream;

public class ScriptingTests {
   public static void loadScript(ScriptingManager scriptingManager, String scriptName) throws IOException {
      try (InputStream is = ScriptingTest.class.getResourceAsStream("/" + scriptName)) {
         String script = TestingUtil.loadFileAsString(is);
         scriptingManager.addScript(scriptName, script);
      }
   }
}
