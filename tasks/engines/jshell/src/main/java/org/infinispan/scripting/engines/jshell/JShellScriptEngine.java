package org.infinispan.scripting.engines.jshell;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Objects;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import javax.script.SimpleScriptContext;

/**
 * Script engine for JShell.
 * @author Eric Obermühlner
 */
public class JShellScriptEngine implements ScriptEngine, Compilable {

   private ScriptContext context = new SimpleScriptContext();

   @Override
   public ScriptContext getContext() {
      return context;
   }

   @Override
   public void setContext(ScriptContext context) {
      Objects.requireNonNull(context);
      this.context = context;
   }

   @Override
   public Bindings createBindings() {
      return new SimpleBindings();
   }

   @Override
   public Bindings getBindings(int scope) {
      return context.getBindings(scope);
   }

   @Override
   public void setBindings(Bindings bindings, int scope) {
      context.setBindings(bindings, scope);
   }

   @Override
   public void put(String key, Object value) {
      getBindings(ScriptContext.ENGINE_SCOPE).put(key, value);
   }

   @Override
   public Object get(String key) {
      return getBindings(ScriptContext.ENGINE_SCOPE).get(key);
   }

   @Override
   public ScriptEngineFactory getFactory() {
      return new JShellScriptEngineFactory();
   }

   @Override
   public Object eval(Reader reader) throws ScriptException {
      return eval(readScript(reader));
   }

   @Override
   public Object eval(String script) throws ScriptException {
      return eval(script, context);
   }

   @Override
   public Object eval(Reader reader, ScriptContext context) throws ScriptException {
      return eval(readScript(reader), context);
   }

   @Override
   public Object eval(String script, ScriptContext context) throws ScriptException {
      return eval(script, context.getBindings(ScriptContext.ENGINE_SCOPE));
   }

   @Override
   public Object eval(Reader reader, Bindings bindings) throws ScriptException {
      return eval(readScript(reader), bindings);
   }

   @Override
   public Object eval(String script, Bindings bindings) throws ScriptException {
      CompiledScript compile = compile(script);

      return compile.eval(bindings);
   }

   @Override
   public CompiledScript compile(Reader reader) throws ScriptException {
      return compile(readScript(reader));
   }

   @Override
   public CompiledScript compile(String script) throws ScriptException {
      return new JShellCompiledScript(this, script);
   }

   private String readScript(Reader reader) throws ScriptException {
      try {
         StringBuilder s = new StringBuilder();
         BufferedReader bufferedReader = new BufferedReader(reader);
         String line;
         while ((line = bufferedReader.readLine()) != null) {
            s.append(line);
            s.append("\n");
         }
         return s.toString();
      } catch (IOException e) {
         throw new ScriptException(e);
      }
   }
}
