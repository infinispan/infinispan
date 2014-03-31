package org.infinispan.configuration.global;

import java.util.Collections;
import java.util.Map;

import org.infinispan.security.AuditLogger;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.Role;

/**
 * GlobalAuthorizationConfiguration.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class GlobalAuthorizationConfiguration {
   private final boolean enabled;
   private final AuditLogger auditLogger;
   private final PrincipalRoleMapper principalRoleMapper;

   private final Map<String, Role> roles;

   public GlobalAuthorizationConfiguration(boolean enabled, AuditLogger auditLogger, PrincipalRoleMapper principalRoleMapper, Map<String, Role> roles) {
      this.enabled = enabled;
      this.auditLogger = auditLogger;
      this.principalRoleMapper = principalRoleMapper;
      this.roles = Collections.unmodifiableMap(roles);
   }

   public boolean enabled() {
      return enabled;
   }


   public AuditLogger auditLogger() {
      return auditLogger;
   }

   public PrincipalRoleMapper principalRoleMapper() {
      return principalRoleMapper;
   }

   public Map<String, Role> roles() {
      return roles;
   }

   @Override
   public String toString() {
      return "GlobalAuthorizationConfiguration [enabled=" + enabled + ", auditLogger=" + auditLogger
            + ", principalRoleMapper=" + principalRoleMapper + ", roles=" + roles + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((auditLogger == null) ? 0 : auditLogger.hashCode());
      result = prime * result + (enabled ? 1231 : 1237);
      result = prime * result + ((principalRoleMapper == null) ? 0 : principalRoleMapper.hashCode());
      result = prime * result + ((roles == null) ? 0 : roles.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      GlobalAuthorizationConfiguration other = (GlobalAuthorizationConfiguration) obj;
      if (auditLogger == null) {
         if (other.auditLogger != null)
            return false;
      } else if (!auditLogger.equals(other.auditLogger))
         return false;
      if (enabled != other.enabled)
         return false;
      if (principalRoleMapper == null) {
         if (other.principalRoleMapper != null)
            return false;
      } else if (!principalRoleMapper.equals(other.principalRoleMapper))
         return false;
      if (roles == null) {
         if (other.roles != null)
            return false;
      } else if (!roles.equals(other.roles))
         return false;
      return true;
   }
}
