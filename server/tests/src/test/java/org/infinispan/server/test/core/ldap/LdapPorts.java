package org.infinispan.server.test.core.ldap;

/**
 * Immutable holder for the host ports allocated for a single LDAP server instance.
 *
 * <p>Every local LDAP backend binds host ports that the server configuration must be
 * able to reach. Ports are allocated per test (see {@code LdapServerListener}) so that
 * backends running in the same JVM fork never collide on a shared fixed port. {@code kdc()}
 * is {@code 0} when no Kerberos KDC is in use.
 */
public final class LdapPorts {

   private final int ldap;
   private final int ldaps;
   private final int kdc;

   public LdapPorts(int ldap, int ldaps, int kdc) {
      this.ldap = ldap;
      this.ldaps = ldaps;
      this.kdc = kdc;
   }

   /**
   * Host port for plain LDAP.
   */
   public int ldap() {
      return ldap;
   }

   /**
   * Host port for LDAPS.
   */
   public int ldaps() {
      return ldaps;
   }

   /**
   * Host port for the Kerberos KDC, or {@code 0} when no KDC is in use.
   */
   public int kdc() {
      return kdc;
   }

   public boolean hasKdc() {
      return kdc > 0;
   }

   @Override
   public String toString() {
      return "LdapPorts[ldap=" + ldap + ", ldaps=" + ldaps + ", kdc=" + kdc + "]";
   }
}
