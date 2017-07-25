package org.infinispan.checkstyle.checks.interceptors;

import static com.puppycrawl.tools.checkstyle.api.TokenTypes.ANNOTATION;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.CLASS_DEF;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.IDENT;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.METHOD_DEF;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.MODIFIERS;
import static com.puppycrawl.tools.checkstyle.api.TokenTypes.OBJBLOCK;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;

import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;

/**
 * Checks that if the interceptor handles one command it handles all conceptually similar ones.
 */
public abstract class AbstractInterceptorCheck extends AbstractCheck {
   protected static Stream<DetailAST> stream(DetailAST ast, int type) {
      Stream.Builder<DetailAST> builder = Stream.builder();
      for (DetailAST child = ast.getFirstChild(); child != null; child = child.getNextSibling()) {
         if (child.getType() == type) {
            builder.accept(child);
         }
      }
      return builder.build();
   }

   @Override
   public int[] getDefaultTokens() {
      return getAcceptableTokens();
   }

   @Override
   public int[] getAcceptableTokens() {
      return new int[] { CLASS_DEF };
   }

   @Override
   public int[] getRequiredTokens() {
      return getAcceptableTokens();
   }

   @Override
   public void visitToken(DetailAST klass) {
      Set<String> containsMethods = new HashSet<>();
      stream(klass, OBJBLOCK).flatMap(objblock -> stream(objblock, METHOD_DEF)).forEach(m -> {
         DetailAST identNode = m.findFirstToken(IDENT);
         if (identNode != null && methods().contains(identNode.getText())) {
            containsMethods.add(identNode.getText());
         }
      });
      if (!containsMethods.isEmpty() && containsMethods.size() != methods().size()) {
         DetailAST modifiers = klass.findFirstToken(MODIFIERS);
         if (modifiers != null) {
            if (stream(modifiers, ANNOTATION).anyMatch(annotation -> {
               DetailAST identNode = annotation.findFirstToken(IDENT);
               return identNode != null && "Deprecated".equals(identNode.getText());
            })) {
               // Don't report deprecated classes
               return;
            }
         }

         Set<String> missingMethods = new HashSet<>(methods());
         missingMethods.removeAll(containsMethods);
         log(klass.getLineNo(), klass.getColumnNo(), "Interceptor defines methods {0}" +
               " but does not define {1} [not required for tests]", containsMethods, missingMethods);

      }
   }

   protected abstract Set<String> methods();
}
