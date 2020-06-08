package org.infinispan.scripting.engines.jshell;

import java.util.Map;

/**
 * Utility to transfer variables in and out of a JShell evaluation.
 * @author Eric Obermühlner
 */
public class VariablesTransfer {
   private static final ThreadLocal<Map<String, Object>> threadLocalVariables = new ThreadLocal<>();

   private VariablesTransfer() {
      // empty
   }

   /**
    * Sets all variables for an evaluation.
    *
    * @param variables the name/value pairs
    */
   public static void setVariables(Map<String, Object> variables) {
      threadLocalVariables.set(variables);
   }

   /**
    * Returns the variable value for the specified name.
    *
    * @param name the name of the variable
    * @return the value of the variable, or <code>null</code> if not defined
    */
   public static Object getVariableValue(String name) {
      return threadLocalVariables.get().get(name);
   }

   /**
    * Sets the variable value for the specified name.
    *
    * @param name the name of the variable
    * @param value the value of the variable
    */
   public static void setVariableValue(String name, Object value) {
      threadLocalVariables.get().put(name, value);
   }

   /**
    * Clears all variables.
    */
   public static void remove() {
      threadLocalVariables.remove();
   }

}
