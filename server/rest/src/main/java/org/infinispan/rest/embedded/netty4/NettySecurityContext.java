package org.infinispan.rest.embedded.netty4;

import java.security.Principal;

import javax.ws.rs.core.SecurityContext;

import org.jboss.resteasy.plugins.server.embedded.SecurityDomain;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 * Temporary fork from RestEasy 3.1.0
 */
public class NettySecurityContext implements SecurityContext {
   public static final NettySecurityContext ANONYMOUS = new NettySecurityContext();
   protected final Principal principal;
   protected final SecurityDomain domain;
   protected final String authScheme;
   protected final boolean isSecure;

   public NettySecurityContext(Principal principal, SecurityDomain domain, String authScheme, boolean secure) {
      this.principal = principal;
      this.domain = domain;
      this.authScheme = authScheme;
      isSecure = secure;
   }

   public NettySecurityContext() {
      this(null, null, null, false);
   }

   @Override
   public Principal getUserPrincipal() {
      return principal;
   }

   @Override
   public boolean isUserInRole(String role) {
      if (domain == null) return false;
      return domain.isUserInRole(principal, role);
   }

   @Override
   public boolean isSecure() {
      return isSecure;
   }

   @Override
   public String getAuthenticationScheme() {
      return authScheme;
   }
}
