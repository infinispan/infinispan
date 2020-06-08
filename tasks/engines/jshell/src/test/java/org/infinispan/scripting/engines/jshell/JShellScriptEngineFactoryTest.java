package org.infinispan.scripting.engines.jshell;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import javax.script.ScriptEngine;

import org.junit.Test;

/**
 * @author Eric Obermühlner
 */
public class JShellScriptEngineFactoryTest {
    @Test
    public void testGetEngineName() {
        JShellScriptEngineFactory factory = new JShellScriptEngineFactory();
        assertEquals("JShell ScriptEngine", factory.getEngineName());
    }

    @Test
    public void testGetEngineVersion() {
        JShellScriptEngineFactory factory = new JShellScriptEngineFactory();
        assertEquals("1.1.0", factory.getEngineVersion());
    }

    @Test
    public void testGetLanguageName() {
        JShellScriptEngineFactory factory = new JShellScriptEngineFactory();
        assertEquals("JShell", factory.getLanguageName());
    }

    @Test
    public void testGetLanguageVersion() {
        JShellScriptEngineFactory factory = new JShellScriptEngineFactory();
        assertEquals(System.getProperty("java.version"), factory.getLanguageVersion());
    }

    @Test
    public void testGetExtensions() {
        JShellScriptEngineFactory factory = new JShellScriptEngineFactory();
        assertEquals(Arrays.asList("jsh", "jshell"), factory.getExtensions());
    }

    @Test
    public void testGetMimeTypes() {
        JShellScriptEngineFactory factory = new JShellScriptEngineFactory();
        assertEquals(Arrays.asList("text/x-jshell-source"), factory.getMimeTypes());
    }

    @Test
    public void testGetNames() {
        JShellScriptEngineFactory factory = new JShellScriptEngineFactory();
        assertEquals(Arrays.asList("JShell", "jshell"), factory.getNames());
    }

    @Test
    public void testGetParameters() {
        JShellScriptEngineFactory factory = new JShellScriptEngineFactory();
        assertThat(factory.getParameter(ScriptEngine.ENGINE)).isEqualTo(factory.getEngineName());
        assertThat(factory.getParameter(ScriptEngine.ENGINE_VERSION)).isEqualTo(factory.getEngineVersion());
        assertThat(factory.getParameter(ScriptEngine.LANGUAGE)).isEqualTo(factory.getLanguageName());
        assertThat(factory.getParameter(ScriptEngine.LANGUAGE_VERSION)).isEqualTo(factory.getLanguageVersion());
        assertThat(factory.getParameter(ScriptEngine.NAME)).isEqualTo("JShell");
        assertThat(factory.getParameter("unknown")).isEqualTo(null);
    }

    @Test
    public void testGetMethodCallSyntax() {
        JShellScriptEngineFactory factory = new JShellScriptEngineFactory();
        assertThat(factory.getMethodCallSyntax("obj", "method")).isEqualTo("obj.method()");
        assertThat(factory.getMethodCallSyntax("obj", "method", "alpha")).isEqualTo("obj.method(alpha)");
        assertThat(factory.getMethodCallSyntax("obj", "method", "alpha", "beta")).isEqualTo("obj.method(alpha,beta)");
    }

    @Test
    public void testGetOutputStatement() {
        JShellScriptEngineFactory factory = new JShellScriptEngineFactory();
        assertThat(factory.getOutputStatement("alpha")).isEqualTo("System.out.println(alpha)");
    }

    @Test
    public void testGetProgram() {
        JShellScriptEngineFactory factory = new JShellScriptEngineFactory();
        assertThat(factory.getProgram()).isEqualTo("");
        assertThat(factory.getProgram("alpha")).isEqualTo("alpha;\n");
        assertThat(factory.getProgram("alpha", "beta")).isEqualTo("alpha;\nbeta;\n");
    }

    @Test
    public void testGetScriptEngine() {
        JShellScriptEngineFactory factory = new JShellScriptEngineFactory();
        assertThat(factory.getScriptEngine() instanceof JShellScriptEngine).isTrue();
    }

}
