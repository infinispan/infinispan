package org.infinispan.checkstyle.checks.interceptors;

import static com.puppycrawl.tools.checkstyle.api.TokenTypes.CLASS_DEF;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.IDENT;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.METHOD_DEF;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.OBJBLOCK;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;

/**
 * Checks that an interceptor overriding {@code visitGetKeyValueCommand} also overrides
 * {@code visitGetCacheEntryCommand} and vice versa.
 * <p>
 * The optimized get interceptor chain navigates both commands via the same linked list, so an interceptor
 * that handles one but not the other will silently be skipped for the missing command.
 * This check should be configured with error severity.
 */
public class InterceptorGetPairCheck extends AbstractCheck {
   private static final Set<String> GET_PAIR = new HashSet<>(Arrays.asList(
         "visitGetKeyValueCommand",
         "visitGetCacheEntryCommand"));

   @Override
   public int[] getDefaultTokens() {
      return new int[] { CLASS_DEF };
   }

   @Override
   public int[] getAcceptableTokens() {
      return getDefaultTokens();
   }

   @Override
   public int[] getRequiredTokens() {
      return getDefaultTokens();
   }

   @Override
   public void visitToken(DetailAST klass) {
      if (System.getProperty("infinispan.checkstyle.interceptors") == null) {
         return;
      }
      Set<String> found = new HashSet<>();
      AbstractInterceptorCheck.stream(klass, OBJBLOCK)
            .flatMap(objblock -> AbstractInterceptorCheck.stream(objblock, METHOD_DEF))
            .forEach(m -> {
               DetailAST identNode = m.findFirstToken(IDENT);
               if (identNode != null && GET_PAIR.contains(identNode.getText())) {
                  found.add(identNode.getText());
               }
            });
      if (found.size() == 1) {
         Set<String> missing = new HashSet<>(GET_PAIR);
         missing.removeAll(found);
         log(klass.getLineNo(), klass.getColumnNo(),
               "Interceptor defines {0} but does not define {1}. " +
               "Both must be overridden for the optimized get interceptor chain.",
               found, missing);
      }
   }
}
