package org.infinispan.spring.common.session;

import static org.springframework.session.FindByIndexNameSessionRepository.PRINCIPAL_NAME_INDEX_NAME;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.session.Session;

/**
 * Extracts Principal Name from Session. This needs to be done separately since Spring Session is not aware of any
 * authentication mechanism (it is application developer's responsibility to implement it).
 *
 * @author Sebastian Łaskawiec
 * @see org.springframework.session.FindByIndexNameSessionRepository
 * @since 9.0
 */
public class PrincipalNameResolver {
   private static final PrincipalNameResolver INSTANCE = new PrincipalNameResolver();

   private static final String SPRING_SECURITY_CONTEXT = "SPRING_SECURITY_CONTEXT";

   private final SpelExpressionParser parser = new SpelExpressionParser();

   public static PrincipalNameResolver getInstance() {
      return INSTANCE;
   }

   /**
    * Resolves Principal Name (e.g. user name) based on session.
    *
    * @param session Session to be checked.
    * @return Extracted Principal Name
    */
   public String resolvePrincipal(Session session) {
      String principalName = session.getAttribute(PRINCIPAL_NAME_INDEX_NAME);
      if (principalName != null) {
         return principalName;
      }
      Object authentication = session.getAttribute(SPRING_SECURITY_CONTEXT);
      if (authentication != null) {
         Expression expression = parser.parseExpression("authentication?.name");
         return expression.getValue(authentication, String.class);
      }
      return null;
   }

}
