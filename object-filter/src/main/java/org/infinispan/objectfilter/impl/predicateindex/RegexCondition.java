package org.infinispan.objectfilter.impl.predicateindex;

import java.util.regex.Pattern;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class RegexCondition extends Condition<String> {

   private enum Type {
      Regexp, Contains, StartsWith, EndsWith, Equals
   }

   private final Type type;
   private final String jpaPattern;
   private final Pattern regexPattern;
   private final String value;
   private int length;

   //todo [anistor] handle org.hibernate.hql.ast.spi.predicate.LikePredicate.escapeCharacter
   public RegexCondition(String jpaPattern) {
      this.jpaPattern = jpaPattern;
      if (jpaPattern.indexOf('%') == -1 && jpaPattern.indexOf('_') == -1) {
         type = Type.Equals;
         value = jpaPattern;
         regexPattern = null;
         length = -1;
      } else {
         if (jpaPattern.charAt(0) == '%') {
            String s = jpaPattern.substring(1);
            if (s.indexOf('%') == -1 && s.indexOf('_') == -1) {
               type = Type.EndsWith;
               value = s;
               length = -1;
               regexPattern = null;
               return;
            } else if (s.indexOf('%') == s.length() - 1 && s.indexOf('_') == -1) {
               type = Type.Contains;
               value = s.substring(0, s.length() - 1);
               length = -1;
               regexPattern = null;
               return;
            }
         } else if (jpaPattern.charAt(0) == '_') {
            String s = jpaPattern.substring(1);
            if (s.indexOf('%') == -1 && s.indexOf('_') == -1) {
               type = Type.EndsWith;
               value = s;
               length = jpaPattern.length();
               regexPattern = null;
               return;
            }
         } else if (jpaPattern.charAt(jpaPattern.length() - 1) == '%') {
            String s = jpaPattern.substring(0, jpaPattern.length() - 1);
            if (s.indexOf('%') == -1 && s.indexOf('_') == -1) {
               type = Type.StartsWith;
               value = s;
               length = -1;
               regexPattern = null;
               return;
            }
         } else if (jpaPattern.charAt(jpaPattern.length() - 1) == '_') {
            String s = jpaPattern.substring(0, jpaPattern.length() - 1);
            if (s.indexOf('%') == -1 && s.indexOf('_') == -1) {
               type = Type.StartsWith;
               value = s;
               length = jpaPattern.length();
               regexPattern = null;
               return;
            }
         }

         type = Type.Regexp;
         regexPattern = Pattern.compile(jpaWildcardToRegex(jpaPattern));
         value = null;
         length = -1;
      }
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

   @Override
   public boolean match(String attributeValue) {
      if (attributeValue == null) {
         return false;
      }

      switch (type) {
         case Equals:
            return attributeValue.equals(value);
         case StartsWith:
            return (length == -1 || length == attributeValue.length()) && attributeValue.startsWith(value);
         case EndsWith:
            return (length == -1 || length == attributeValue.length()) && attributeValue.endsWith(value);
         case Contains:
            return attributeValue.contains(value);
         case Regexp:
            return regexPattern.matcher(attributeValue).matches();
         default:
            throw new IllegalStateException("Unexpected type " + type);
      }
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;

      RegexCondition other = (RegexCondition) obj;
      return jpaPattern.equals(other.jpaPattern);
   }

   @Override
   public int hashCode() {
      return jpaPattern.hashCode();
   }

   @Override
   public String toString() {
      return "RegexCondition(" + jpaPattern + ')';
   }
}
