package org.infinispan.checkstyle.checks;

import java.util.regex.Pattern;

import com.puppycrawl.tools.checkstyle.api.AbstractCheck;
import com.puppycrawl.tools.checkstyle.api.DetailAST;
import com.puppycrawl.tools.checkstyle.api.TokenTypes;

/**
 * Checks that certain methods are not invoked
 *
 * The methodPattern argument is a regular expression, invocations of method names matching the pattern are forbidden.
 * Only method names are checked, the defining class doesn't matter.
 *
 * If the optional argumentCount argument is set, only overloads with that number of parameters are forbidden.
 * Forbidding only overloads with 0 arguments is not supported at this time.
 *
 * @author wburns
 * @since 10.0
 */

public class ForbiddenMethodCheck extends AbstractCheck {
   private Pattern methodPattern = null;
   private int argumentCount = -1;

   @Override
   public int[] getDefaultTokens() {
      return new int[] { TokenTypes.METHOD_CALL };
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
   public void init() {
      if (methodPattern == null) {
         throw new IllegalStateException("The methodPattern attribute is required!");
      }
   }

   public void setMethodPattern(String methodPatternString) {
      methodPattern = Pattern.compile(methodPatternString);
   }

   public void setArgumentCount(String argumentCountString) {
      argumentCount = Integer.parseInt(argumentCountString);
      if (argumentCount <= 0) {
         throw new IllegalArgumentException("Argument count " + argumentCount + " was 0 or negative");
      }
   }

   @Override
   public void visitToken(DetailAST ast) {
      boolean nameMatches = false;
      for (DetailAST child = ast.getFirstChild(); child != null; child = child.getNextSibling()) {
         int childType = child.getType();
         switch (childType) {
            // variable.method invocation
            case TokenTypes.DOT:
               DetailAST methodAST = child.getLastChild();
               if (methodAST.getType() == TokenTypes.IDENT) {
                  String methodName = methodAST.getText();
                  if (methodPattern.matcher(methodName).matches()) {
                     if (argumentCount > 0) {
                        nameMatches = true;
                     } else {
                        log(ast, "[not required for tests] Forbidden method invocation found that matches {0}", methodPattern.pattern());
                     }
                  }
               }
               break;
            case TokenTypes.ELIST:
               if (nameMatches) {
                  int count = child.getChildCount(TokenTypes.COMMA) + 1;
                  if (argumentCount == count) {
                     log(ast, "[not required for tests] Forbidden method invocation found that matches {0} with {1} number of arguments",
                           methodPattern.pattern(), argumentCount);
                  }
               }
               break;
         }
      }
   }
}
