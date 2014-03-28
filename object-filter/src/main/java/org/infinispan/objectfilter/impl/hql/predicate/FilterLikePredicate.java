package org.infinispan.objectfilter.impl.hql.predicate;

import org.hibernate.hql.ast.spi.predicate.LikePredicate;
import org.infinispan.objectfilter.impl.syntax.BooleanExpr;
import org.infinispan.objectfilter.impl.syntax.PropertyValueExpr;
import org.infinispan.objectfilter.impl.syntax.RegexExpr;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class FilterLikePredicate extends LikePredicate<BooleanExpr> {

   public FilterLikePredicate(String propertyName, String patternValue) {
      super(propertyName, patternValue, null);
   }

   @Override
   public BooleanExpr getQuery() {
      //todo [anistor] handle LikePredicate.escapeCharacter
      return new RegexExpr(new PropertyValueExpr(propertyName), jpaWildcardToRegex(patternValue));
   }

   private String jpaWildcardToRegex(String jpaPattern) {
      StringBuilder sb = new StringBuilder(jpaPattern.length());
      sb.append('^');
      for (int i = 0; i < jpaPattern.length(); i++) {
         char c = jpaPattern.charAt(i);
         switch (c) {
            case '%':
               sb.append(".*");
               break;

            case '_':
               sb.append('.');
               break;

            // regexp special characters need to be escaped
            case '(':
            case ')':
            case '[':
            case ']':
            case '$':
            case '^':
            case '.':
            case '{':
            case '}':
            case '|':
            case '\\':
               sb.append('\\');
               // intended fall through

            default:
               sb.append(c);
               break;
         }
      }
      sb.append('$');
      return sb.toString();
   }
}
