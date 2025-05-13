package org.infinispan.security.impl;

import java.util.Objects;

import javax.security.auth.Subject;

/**
 * CachePrincipalPair.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
public final class CacheSubjectPair {
   private final Subject subject;
   private final String cacheName;
   private final int hashCode;

   CacheSubjectPair(Subject subject, String cacheName) {
      this.subject = subject;
      this.cacheName = cacheName;
      this.hashCode = computeHashCode();
   }

   private int computeHashCode() {
      return Objects.hash(subject, cacheName);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CacheSubjectPair that = (CacheSubjectPair) o;
      return subject.equals(that.subject) && cacheName.equals(that.cacheName);
   }

   @Override
   public int hashCode() {
      return hashCode;
   }

   @Override
   public String toString() {
      return "CacheSubjectPair{" +
            "cacheName=" + cacheName +
            ", subject=" + subject +
            '}';
   }
}
