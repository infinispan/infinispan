package org.infinispan.security.impl;

import java.security.AccessControlContext;
import java.security.AccessControlException;
import java.security.AccessController;
import java.security.Principal;
import java.util.Set;

import javax.security.auth.Subject;

import org.infinispan.configuration.cache.AuthorizationConfiguration;
import org.infinispan.configuration.global.GlobalSecurityConfiguration;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuditLogger;
import org.infinispan.security.AuditResponse;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.Role;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * AuthorizationHelper. Some utility methods for computing access masks and verifying them against
 * permissions
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class AuthorizationHelper {
   private static final Log log = LogFactory.getLog(AuthorizationHelper.class);
   private final GlobalSecurityConfiguration globalConfiguration;
   private final AuditLogger audit;
   private final AuditContext context;
   private final String name;

   public AuthorizationHelper(GlobalSecurityConfiguration globalConfiguration, AuditContext context, String name) {
      this.globalConfiguration = globalConfiguration;
      this.audit = globalConfiguration.authorization().auditLogger();
      this.context = context;
      this.name = name;
   }

   public void checkPermission(Subject subject, int subjectMask, AuthorizationPermission perm) {
      if ((subjectMask & perm.getMask()) != perm.getMask()) {
         if (System.getSecurityManager() == null) {
            try {
               if (subject == null) {
                  AccessController.getContext().checkPermission(perm.getSecurityPermission());
               } else {
                  throw new AccessControlException(perm.toString());
               }
            } catch (AccessControlException ace) {
               audit.audit(subject, context, name, perm, AuditResponse.DENY);
               throw log.unauthorizedAccess(String.valueOf(subject), perm.toString());
            }
         } else {
            System.getSecurityManager().checkPermission(perm.getSecurityPermission());
         }
      }
      audit.audit(subject, context, name, perm, AuditResponse.ALLOW);
   }

   public void checkPermission(AuthorizationConfiguration configuration, AuthorizationPermission perm) {
      if (globalConfiguration.authorization().enabled()) {
         AccessControlContext acc = AccessController.getContext();
         Subject subject = Subject.getSubject(acc);
         int subjectMask = computeSubjectRoleMask(subject, globalConfiguration, configuration);
         checkPermission(subject, subjectMask, perm);
      }
   }

   public void checkPermission(AuthorizationPermission perm) {
      checkPermission(null, perm);
   }

   public static int computeSubjectRoleMask(Subject subject, GlobalSecurityConfiguration globalConfiguration, AuthorizationConfiguration configuration) {
      PrincipalRoleMapper roleMapper = globalConfiguration.authorization().principalRoleMapper();
      int mask = 0;
      if (subject != null) {
         for (Principal principal : subject.getPrincipals()) {
            Set<String> roleNames = roleMapper.principalToRoles(principal);
            if (roleNames != null) {
               for (String roleName : roleNames) {
                  // Skip roles not defined for this cache
                  if (configuration != null && !configuration.roles().contains(roleName))
                     continue;
                  Role role = globalConfiguration.authorization().roles().get(roleName);
                  if (role != null) {
                     mask |= role.getMask();
                  }
               }
            }
         }
      }
      return mask;
   }
}
