package org.infinispan.security.mappers;

/**
 * @since 15.0
 **/
@FunctionalInterface
public interface NameRewriter {
   NameRewriter IDENTITY_REWRITER = original -> original;

   /**
    * Rewrite a name.  Must not return {@code null}.
    *
    * @param original the original name (must not be {@code null})
    * @return the rewritten name, or {@code null} if the name is invalid
    */
   String rewriteName(String original);
}
