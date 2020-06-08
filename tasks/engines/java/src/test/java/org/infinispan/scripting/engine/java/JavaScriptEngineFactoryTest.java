package org.infinispan.scripting.engine.java;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import javax.script.ScriptEngine;

import org.junit.Test;

public class JavaScriptEngineFactoryTest {
   @Test
   public void testGetEngineName() {
      JavaScriptEngineFactory factory = new JavaScriptEngineFactory();
      assertThat(factory.getEngineName()).isEqualTo("Java ScriptEngine");
   }

   @Test
   public void testGetEngineVersion() {
      JavaScriptEngineFactory factory = new JavaScriptEngineFactory();
      assertThat(factory.getEngineVersion()).isEqualTo("2.0.0");
   }

   @Test
   public void testGetLanguageName() {
      JavaScriptEngineFactory factory = new JavaScriptEngineFactory();
      assertThat(factory.getLanguageName()).isEqualTo("Java");
   }

   @Test
   public void testGetLanguageVersion() {
      JavaScriptEngineFactory factory = new JavaScriptEngineFactory();
      assertThat(factory.getLanguageVersion()).isEqualTo(System.getProperty("java.version"));
   }

   @Test
   public void testGetExtensions() {
      JavaScriptEngineFactory factory = new JavaScriptEngineFactory();
      assertThat(factory.getExtensions()).isEqualTo(Arrays.asList("java"));
   }

   @Test
   public void testGetMimeTypes() {
      JavaScriptEngineFactory factory = new JavaScriptEngineFactory();
      assertThat(factory.getMimeTypes()).isEqualTo(Arrays.asList("text/x-java-source"));
   }

   @Test
   public void testGetNames() {
      JavaScriptEngineFactory factory = new JavaScriptEngineFactory();
      assertThat(factory.getNames()).isEqualTo(Arrays.asList("Java", "java"));
   }

   @Test
   public void testGetParameters() {
      JavaScriptEngineFactory factory = new JavaScriptEngineFactory();
      assertThat(factory.getParameter(ScriptEngine.ENGINE)).isEqualTo(factory.getEngineName());
      assertThat(factory.getParameter(ScriptEngine.ENGINE_VERSION)).isEqualTo(factory.getEngineVersion());
      assertThat(factory.getParameter(ScriptEngine.LANGUAGE)).isEqualTo(factory.getLanguageName());
      assertThat(factory.getParameter(ScriptEngine.LANGUAGE_VERSION)).isEqualTo(factory.getLanguageVersion());
      assertThat(factory.getParameter(ScriptEngine.NAME)).isEqualTo("Java");
      assertThat(factory.getParameter("unknown")).isEqualTo(null);
   }

   @Test
   public void testGetMethodCallSyntax() {
      JavaScriptEngineFactory factory = new JavaScriptEngineFactory();
      assertThat(factory.getMethodCallSyntax("obj", "method")).isEqualTo("obj.method()");
      assertThat(factory.getMethodCallSyntax("obj", "method", "alpha")).isEqualTo("obj.method(alpha)");
      assertThat(factory.getMethodCallSyntax("obj", "method", "alpha", "beta")).isEqualTo("obj.method(alpha,beta)");
   }

   @Test
   public void testGetOutputStatement() {
      JavaScriptEngineFactory factory = new JavaScriptEngineFactory();
      assertThat(factory.getOutputStatement("alpha")).isEqualTo("System.out.println(alpha)");
   }

   @Test
   public void testGetProgram() {
      JavaScriptEngineFactory factory = new JavaScriptEngineFactory();
      assertThat(factory.getProgram()).isEqualTo("");
      assertThat(factory.getProgram("alpha")).isEqualTo("alpha;\n");
      assertThat(factory.getProgram("alpha", "beta")).isEqualTo("alpha;\nbeta;\n");
   }

   @Test
   public void testGetScriptEngine() {
      JavaScriptEngineFactory factory = new JavaScriptEngineFactory();
      assertThat(factory.getScriptEngine() instanceof JavaScriptEngine).isTrue();
   }

}
