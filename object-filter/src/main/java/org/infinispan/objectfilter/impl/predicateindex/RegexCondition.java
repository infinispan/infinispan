package org.infinispan.objectfilter.impl.predicateindex;

import java.util.regex.Pattern;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class RegexCondition extends Condition<String> {

   private final Pattern pattern;

   public RegexCondition(Pattern pattern) {
      this.pattern = pattern;
   }

   @Override
   public boolean match(String attributeValue) {
      return attributeValue != null && pattern.matcher(attributeValue).matches();
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null || getClass() != obj.getClass()) return false;

      RegexCondition other = (RegexCondition) obj;
      return pattern.equals(other.pattern);
   }

   @Override
   public int hashCode() {
      return pattern.hashCode();
   }

   @Override
   public String toString() {
      return "RegexCondition(" + pattern + ')';
   }
}
