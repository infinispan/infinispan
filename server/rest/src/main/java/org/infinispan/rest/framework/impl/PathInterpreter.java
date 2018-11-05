package org.infinispan.rest.framework.impl;

import static org.infinispan.rest.framework.impl.PathInterpreter.State.END_VAR;
import static org.infinispan.rest.framework.impl.PathInterpreter.State.TEXT;
import static org.infinispan.rest.framework.impl.PathInterpreter.State.VAR;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @since 10.0
 */
class PathInterpreter {

   enum State {VAR, END_VAR, TEXT}

   /**
    * Attempts to resolve an expression against a String.
    * Example, an expression '{variable}' and a String 'value' yields Map(variable=value)
    *
    * @return each resolved variable with its value, or empty map if the expression is not compatible with the supplied string.
    */
   static Map<String, String> resolveVariables(String expression, String str) {
      if (expression == null || str == null) return Collections.emptyMap();

      Map<String, String> resolvedVariables = new HashMap<>();
      StringBuilder variableBuilder = new StringBuilder();
      State state = State.TEXT;
      int j = 0;
      int expressionLength = expression.length();

      for (int i = 0; i < expressionLength; i++) {
         char e = expression.charAt(i);

         switch (e) {
            case '{':
               if (state == END_VAR) return Collections.emptyMap();
               state = VAR;
               break;
            case '}':
               if (state != VAR) return Collections.emptyMap();
               state = END_VAR;
               if (i != expressionLength - 1) break;
            default:
               switch (state) {
                  case VAR:
                     variableBuilder.append(e);
                     break;
                  case END_VAR:
                     String replacement;
                     boolean ec = i == expressionLength - 1;
                     if (ec) {
                        replacement = str.substring(j);
                     } else {
                        int k = str.indexOf(e, j);
                        if (k == -1) return Collections.emptyMap();
                        replacement = str.substring(j, str.indexOf(e, j));
                     }
                     resolvedVariables.put(variableBuilder.toString(), replacement);
                     j += replacement.length();
                     if (j == str.length() && ec) return resolvedVariables;
                     variableBuilder.setLength(0);
                     state = TEXT;
                  case TEXT:
                     if (str.charAt(j) != e) return Collections.emptyMap();
                     j++;
               }
         }
      }
      return resolvedVariables;
   }


}
