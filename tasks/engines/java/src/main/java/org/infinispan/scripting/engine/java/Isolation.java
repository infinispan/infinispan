package org.infinispan.scripting.engine.java;

/**
 * The isolation levels of the script at execution time.
 * @author Eric Obermühlner
 */
public enum Isolation {
   /**
    * The caller {@link ClassLoader} is visible to the script during execution.
    * <p>
    * This allows to see all classes from the script that are visible in the calling application.
    */
   CallerClassLoader,

   /**
    * The script executes in an isolated {@link ClassLoader}.
    * <p>
    * This hides all classes of the calling application.
    */
   IsolatedClassLoader
}
