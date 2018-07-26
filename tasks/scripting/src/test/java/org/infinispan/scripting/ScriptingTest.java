package org.infinispan.scripting;

import static org.infinispan.scripting.utils.ScriptingUtils.loadData;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.tasks.TaskContext;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "scripting.ScriptingTest")
@CleanupAfterMethod
public class ScriptingTest extends AbstractScriptingTest {

   static final String CACHE_NAME = "script-exec";

   protected String[] getScripts() {
      return new String[]{"test.js", "testMissingMetaProps.js", "testExecWithoutProp.js", "testInnerScriptCall.js"};
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      builder.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      cacheManager.defineConfiguration(CACHE_NAME, builder.build());
   }

   @Override
   protected void clearContent() {
      cacheManager.getCache().clear();
   }

   public void testSimpleScript() throws Exception {
      String result = (String) scriptingManager.runScript("test.js", new TaskContext().addParameter("a", "a")).get();
      assertEquals("a", result);
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*No script named.*")
   public void testScriptRemove() throws Exception {
      scriptingManager.getScript("testExecWithoutProp.js");
      scriptingManager.removeScript("testExecWithoutProp.js");
      scriptingManager.getScript("testExecWithoutProp.js");
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*No script named.*")
   public void testRunNonExistentScript() throws Exception {
      String result = (String) scriptingManager.runScript("nonExistent.js", new TaskContext().addParameter("a", "a")).get();
      assertEquals("a", result);
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*Script execution error.*")
   public void testSimpleScriptWitoutPassingParameter() throws Exception {
      String result = (String) scriptingManager.runScript("test.js").get();
      assertEquals("a", result);
   }

   public void testSimpleScriptReplacementWithNew() throws ExecutionException, InterruptedException, IOException {
      String result = (String) scriptingManager.runScript("test.js", new TaskContext().addParameter("a", "a")).get();
      assertEquals("a", result);

      //Replacing the existing script with new one.
      InputStream is = this.getClass().getResourceAsStream("/test1.js");
      String script = TestingUtil.loadFileAsString(is);

      scriptingManager.addScript("test.js", script);
      result = (String) scriptingManager.runScript("test.js").get();
      assertEquals("a:modified", result);

      //Rolling back the replacement.
      is = this.getClass().getResourceAsStream("/test.js");
      script = TestingUtil.loadFileAsString(is);

      scriptingManager.addScript("test.js", script);
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*No script named.*")
   public void testScriptingCacheClear() throws Exception {
      String result = (String) scriptingManager.runScript("test.js", new TaskContext().addParameter("a", "a")).get();
      assertEquals("a", result);

      cache(ScriptingManager.SCRIPT_CACHE).clear();

      result = (String) scriptingManager.runScript("test.js", new TaskContext().addParameter("a", "a")).get();
      assertEquals("a", result);
   }

   public void testScriptingCacheManualReplace() throws Exception {
      String result = (String) scriptingManager.runScript("test.js", new TaskContext().addParameter("a", "a")).get();
      assertEquals("a", result);

      //Replacing the existing script with new one.
      InputStream is = this.getClass().getResourceAsStream("/test1.js");
      String script = TestingUtil.loadFileAsString(is);

      cache(ScriptingManager.SCRIPT_CACHE).replace("test.js", script);

      result = (String) scriptingManager.runScript("test.js").get();
      assertEquals("a:modified", result);

      //Rolling back the replacement.
      is = this.getClass().getResourceAsStream("/test.js");
      script = TestingUtil.loadFileAsString(is);

      scriptingManager.addScript("test.js", script);
   }

   public void testSimpleScript1() throws Exception {
      String value = "javaValue";
      String key = "processValue";

      cacheManager.getCache(CACHE_NAME).put(key, value);

      CompletableFuture exec = scriptingManager.runScript("testExecWithoutProp.js");
      exec.get(1000, TimeUnit.MILLISECONDS);

      assertEquals(value + ":additionFromJavascript", cacheManager.getCache(CACHE_NAME).get(key));
   }

   public void testScriptCallFromJavascript() throws Exception {
      String result = (String) scriptingManager.runScript("testInnerScriptCall.js",
            new TaskContext().cache(cacheManager.getCache(CACHE_NAME)).addParameter("a", "ahoj")).get();

      assertEquals("script1:additionFromJavascript", result);
      assertEquals("ahoj", cacheManager.getCache(CACHE_NAME).get("a"));
   }

   public void testSimpleScriptWithMissingLanguageInMetaPropeties() throws Exception {
      String result = (String) scriptingManager.runScript("testMissingMetaProps.js", new TaskContext().addParameter("a", "a")).get();
      assertEquals("a", result);
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*No script named.*")
   public void testRemovingNonExistentScript() {
      scriptingManager.removeScript("nonExistent");
   }

   public void testRemovingScript() throws IOException, ExecutionException, InterruptedException {
      assertNotNull(scriptingManager.getScript("test.js"));

      scriptingManager.removeScript("test.js");
      assertNull(cacheManager.getCache(ScriptingManager.SCRIPT_CACHE).get("test.js"));

      InputStream is = this.getClass().getResourceAsStream("/test.js");
      String script = TestingUtil.loadFileAsString(is);

      scriptingManager.addScript("test.js", script);
      assertNotNull(scriptingManager.getScript("test.js"));
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*Script execution error.*")
   public void testWrongJavaRef() throws Exception {
      InputStream is = this.getClass().getResourceAsStream("/testWrongJavaRef.js");
      String script = TestingUtil.loadFileAsString(is);

      scriptingManager.addScript("testWrongJavaRef.js", script);

      String result = (String) scriptingManager.runScript("testWrongJavaRef.js", new TaskContext().addParameter("a", "a")).get();
      assertEquals("a", result);
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*Script execution error.*")
   public void testWrongPropertyRef() throws Exception {
      InputStream is = this.getClass().getResourceAsStream("/testWrongPropertyRef.js");
      String script = TestingUtil.loadFileAsString(is);

      scriptingManager.addScript("testWrongPropertyRef.js", script);

      String result = (String) scriptingManager.runScript("testWrongPropertyRef.js").get();
      assertEquals("a", result);
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*Compiler error for script.*")
   public void testJsCompilationError() throws Exception {
      InputStream is = this.getClass().getResourceAsStream("/testJsCompilationError.js");
      String script = TestingUtil.loadFileAsString(is);

      scriptingManager.addScript("testJsCompilationError.js", script);

      String result = (String) scriptingManager.runScript("testJsCompilationError.js").get();
      assertEquals("a", result);
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*No script named.*")
   public void testGetNonExistentScript() {
      scriptingManager.getScript("nonExistent.js");
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = ".*Cannot find an appropriate script engine for script.*")
   public void testNonSupportedScript() {
      scriptingManager.addScript("Test.java", "//mode=local,language=java\n" +
            "public class Test {\n" +
            "      public static void main(String[] args) {\n" +
            "         System.out.println(cache.get(\"test.js\"));\n" +
            "      }\n" +
            "   }");

      scriptingManager.runScript("Test.java");
   }

   public void testMapReduceScript() throws IOException, ExecutionException, InterruptedException {
      InputStream is = this.getClass().getResourceAsStream("/wordCountStream.js");
      String script = TestingUtil.loadFileAsString(is);
      loadData(cache(), "/macbeth.txt");

      scriptingManager.addScript("wordCountStream.js", script);
      Map<String, Long> result = (Map<String, Long>) scriptingManager.runScript("wordCountStream.js", new TaskContext().cache(cache())).get();
      assertEquals(3202, result.size());
      assertEquals(Long.valueOf(287), result.get("macbeth"));
   }
}
