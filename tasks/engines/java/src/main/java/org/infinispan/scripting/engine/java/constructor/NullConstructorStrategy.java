package org.infinispan.scripting.engine.java.constructor;

import javax.script.ScriptException;

import org.infinispan.scripting.engine.java.JavaCompiledScript;

/**
 * A {@link ConstructorStrategy} implementation that always returns {@code null}.
 * <p>
 * Used to indicate that only static methods should be called to evaluate the {@link JavaCompiledScript} holding the
 * {@link Class}.
 *
 * @author Eric Obermühlner
 */
public class NullConstructorStrategy implements ConstructorStrategy {
   @Override
   public Object construct(Class<?> clazz) throws ScriptException {
      return null;
   }
}
