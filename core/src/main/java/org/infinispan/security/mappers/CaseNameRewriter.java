package org.infinispan.security.mappers;

import java.util.Locale;

/**
 * A rewriter which can convert a name to uppercase or lowercase
 */
public final class CaseNameRewriter implements NameRewriter {
   private final boolean upperCase;

   public CaseNameRewriter() {
      this(true);
   }

   public CaseNameRewriter(boolean upperCase) {
      this.upperCase = upperCase;
   }

   public String rewriteName(String original) {
      if (original == null) {
         return null;
      } else {
         return this.upperCase ? original.toUpperCase(Locale.ROOT) : original.toLowerCase(Locale.ROOT);
      }
   }
}
