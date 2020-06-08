package org.infinispan.scripting.engine.java;

import java.util.Arrays;
import java.util.List;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;

import org.kohsuke.MetaInfServices;

/**
 * Factory for the {@link JavaScriptEngine}.
 * @author Eric Obermühlner
 */
@MetaInfServices(ScriptEngineFactory.class)
public class JavaScriptEngineFactory implements ScriptEngineFactory {
   @Override
   public String getEngineName() {
      return "Java ScriptEngine";
   }

   @Override
   public String getEngineVersion() {
      return "2.0.0";
   }

   @Override
   public List<String> getExtensions() {
      return Arrays.asList("java");
   }

   @Override
   public List<String> getMimeTypes() {
      return Arrays.asList("text/x-java-source");
   }

   @Override
   public List<String> getNames() {
      return Arrays.asList("Java", "java");
   }

   @Override
   public String getLanguageName() {
      return "Java";
   }

   @Override
   public String getLanguageVersion() {
      return System.getProperty("java.version");
   }

   @Override
   public Object getParameter(String key) {
      switch (key) {
         case ScriptEngine.ENGINE:
            return getEngineName();
         case ScriptEngine.ENGINE_VERSION:
            return getEngineVersion();
         case ScriptEngine.LANGUAGE:
            return getLanguageName();
         case ScriptEngine.LANGUAGE_VERSION:
            return getLanguageVersion();
         case ScriptEngine.NAME:
            return getNames().get(0);
         default:
            return null;
      }
   }

   @Override
   public String getMethodCallSyntax(String obj, String method, String... args) {
      StringBuilder s = new StringBuilder();
      s.append(obj);
      s.append(".");
      s.append(method);
      s.append("(");
      for (int i = 0; i < args.length; i++) {
         if (i > 0) {
            s.append(",");
         }
         s.append(args[i]);
      }
      s.append(")");
      return s.toString();
   }

   @Override
   public String getOutputStatement(String toDisplay) {
      return "System.out.println(" + toDisplay + ")";
   }

   @Override
   public String getProgram(String... statements) {
      StringBuilder s = new StringBuilder();
      for (String statement : statements) {
         s.append(statement);
         s.append(";\n");
      }
      return s.toString();
   }

   @Override
   public ScriptEngine getScriptEngine() {
      return new JavaScriptEngine();
   }
}
