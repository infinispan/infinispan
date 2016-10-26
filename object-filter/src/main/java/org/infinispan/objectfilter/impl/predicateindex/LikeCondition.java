package org.infinispan.objectfilter.impl.predicateindex;

import java.util.BitSet;
import java.util.regex.Pattern;

import org.infinispan.objectfilter.impl.syntax.LikeExpr;

/**
 * A condition that matches String values using a pattern containing the well known '%' and '_' wildcards.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class LikeCondition implements Condition<String> {

   /**
    * The actual match might be a regexp, or possibly a simpler degenerated case.
    */
   private enum Type {
      Regexp, Contains, StartsWith, EndsWith, Equals
   }

   /**
    * The type of this matcher.
    */
   private final Type type;

   /**
    * The original 'like' pattern.
    */
   private final String likePattern;

   /**
    * The regexp. Only used if the match requires full regexp power.
    */
   private final Pattern regexPattern;

   /**
    * The value to match against, or {@code null} if this is based on regexp.
    */
   private final String value;

   /**
    * The expected length of the input value or {@code -1} if the expected length is not constant or if the match is
    * based on regexp.
    */
   private final int length;

   public LikeCondition(String likePattern) {
      this(likePattern, LikeExpr.DEFAULT_ESCAPE_CHARACTER);
   }

   public LikeCondition(String likePattern, char escapeCharacter) {
      this.likePattern = likePattern;

      // before going the full-fledged regexp way we investigate to see if this can be turned into a simpler and less costly string match test

      StringBuilder sb = new StringBuilder(likePattern.length());
      BitSet multi = new BitSet(likePattern.length());
      int multiCount = 0;
      BitSet single = new BitSet(likePattern.length());
      int singleCount = 0;
      boolean isEscaped = false;
      for (int i = 0; i < likePattern.length(); i++) {
         char c = likePattern.charAt(i);
         if (!isEscaped && c == escapeCharacter) {
            isEscaped = true;
         } else {
            if (isEscaped) {
               isEscaped = false;
            } else {
               if (c == LikeExpr.MULTIPLE_CHARACTERS_WILDCARD) {
                  multi.set(sb.length());
                  multiCount++;
               } else if (c == LikeExpr.SINGLE_CHARACTER_WILDCARD) {
                  single.set(sb.length());
                  singleCount++;
               }
            }
            sb.append(c);
         }
      }

      if (multiCount == 0 && singleCount == 0) {
         // no wildcards at all
         type = Type.Equals;
         value = sb.toString();
         regexPattern = null;
         length = -1;
      } else {
         if (multi.get(0)) {
            if (singleCount == 0) {
               if (multiCount == 1) {
                  type = Type.EndsWith;
                  value = sb.substring(1);
                  length = -1;
                  regexPattern = null;
                  return;
               } else if (multiCount == 2 && multi.get(sb.length() - 1)) {
                  type = Type.Contains;
                  value = sb.substring(1, sb.length() - 1);
                  length = -1;
                  regexPattern = null;
                  return;
               }
            }
         } else if (single.get(0)) {
            if (multiCount == 0 && singleCount == 1) {
               type = Type.EndsWith;
               value = sb.substring(1);
               length = sb.length();
               regexPattern = null;
               return;
            }
         } else if (multi.get(sb.length() - 1)) {
            if (multiCount == 1 && singleCount == 0) {
               type = Type.StartsWith;
               value = sb.substring(0, sb.length() - 1);
               length = -1;
               regexPattern = null;
               return;
            }
         } else if (single.get(sb.length() - 1)) {
            if (singleCount == 1 && multiCount == 0) {
               type = Type.StartsWith;
               value = sb.substring(0, sb.length() - 1);
               length = sb.length();
               regexPattern = null;
               return;
            }
         }

         // could not turn it into a degenerated case, so go full regexp
         StringBuilder regexpPattern = new StringBuilder(sb.length() + 2);
         // we match the entire value, from start to end
         regexpPattern.append('^');
         for (int i = 0; i < sb.length(); i++) {
            if (multi.get(i)) {
               regexpPattern.append(".*");
            } else if (single.get(i)) {
               regexpPattern.append('.');
            } else {
               // regexp special characters need to be escaped
               char c = sb.charAt(i);
               switch (c) {
                  case '+':
                  case '*':
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
                     regexpPattern.append('\\');
                     // intended fall through

                  default:
                     regexpPattern.append(c);
               }
            }
         }
         regexpPattern.append('$');

         type = Type.Regexp;
         regexPattern = Pattern.compile(regexpPattern.toString());
         value = null;
         length = -1;
      }
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

      LikeCondition other = (LikeCondition) obj;
      return likePattern.equals(other.likePattern);
   }

   @Override
   public int hashCode() {
      return likePattern.hashCode();
   }

   @Override
   public String toString() {
      return "LikeCondition(" + likePattern + ')';
   }
}
