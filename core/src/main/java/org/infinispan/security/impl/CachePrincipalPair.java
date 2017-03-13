package org.infinispan.security.impl;

import java.security.Principal;

/**
 * CachePrincipalPair.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
final public class CachePrincipalPair {
   private final String principalName;
   private final String cacheName;
   private final int hashCode;

   CachePrincipalPair(Principal userPrincipal, String cacheName) {
      this.principalName = userPrincipal.getName();
      this.cacheName = cacheName;
      this.hashCode = computeHashCode();
   }

   private int computeHashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((cacheName == null) ? 0 : cacheName.hashCode());
      result = prime * result + ((principalName == null) ? 0 : principalName.hashCode());
      return result;
   }

   @Override
   public int hashCode() {
      return hashCode;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      CachePrincipalPair other = (CachePrincipalPair) obj;
      if (!cacheName.equals(other.cacheName))
         return false;
      if (!principalName.equals(other.principalName))
         return false;
      return true;
   }

   @Override
   public String toString() {
      return "CachePrincipalPair{" +
            "cacheName=" + cacheName +
            ", principalName=" + principalName +
            '}';
   }
}
