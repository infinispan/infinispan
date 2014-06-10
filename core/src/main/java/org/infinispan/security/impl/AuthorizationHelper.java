package org.infinispan.security.impl;

import java.security.AccessControlException;
import java.security.Principal;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.security.auth.Subject;

import org.infinispan.configuration.cache.AuthorizationConfiguration;
import org.infinispan.configuration.global.GlobalSecurityConfiguration;
import org.infinispan.registry.ClusterRegistry;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuditLogger;
import org.infinispan.security.AuditResponse;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.Role;
import org.infinispan.security.Security;
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
   private final ClusterRegistry<String, Subject, Integer> maskCache;
   private final String maskCacheScope;

   public AuthorizationHelper(GlobalSecurityConfiguration globalConfiguration, AuditContext context, String name, ClusterRegistry<String, Subject, Integer> clusterRegistry) {
      this.globalConfiguration = globalConfiguration;
      this.audit = globalConfiguration.authorization().auditLogger();
      this.context = context;
      this.name = name;
      this.maskCache = clusterRegistry;
      this.maskCacheScope = AuthorizationManager.class.getSimpleName() + "_" + name;
   }

   public AuthorizationHelper(GlobalSecurityConfiguration globalConfiguration, AuditContext context, String name) {
      this(globalConfiguration, context, name, null);
   }

   public void checkPermission(AuthorizationPermission perm) {
      checkPermission(null, perm);
   }

   public void checkPermission(AuthorizationConfiguration configuration, AuthorizationPermission perm) {
      if (globalConfiguration.authorization().enabled()) {
         if (Security.isPrivileged()) {
            Security.checkPermission(perm.getSecurityPermission());
         } else {
            Subject subject = Security.getSubject();
            try {
               if (subject != null) {
                  int subjectMask = computeSubjectRoleMask(subject, configuration);
                  if ((subjectMask & perm.getMask()) != perm.getMask()) {
                     checkSecurityManagerPermission(perm);
                  } else {
                     audit.audit(subject, context, name, perm, AuditResponse.ALLOW);
                  }
               } else {
                  checkSecurityManagerPermission(perm);
               }
            } catch (SecurityException e) {
               audit.audit(subject, context, name, perm, AuditResponse.DENY);
               throw log.unauthorizedAccess(String.valueOf(subject), perm.toString());
            }
         }
      }
   }

   private void checkSecurityManagerPermission(AuthorizationPermission perm) {
      if (System.getSecurityManager() != null) {
         System.getSecurityManager().checkPermission(perm.getSecurityPermission());
      } else {
         throw new AccessControlException("", perm.getSecurityPermission());
      }
   }

   public int computeSubjectRoleMask(Subject subject, AuthorizationConfiguration configuration) {
      if (subject != null) {
         Integer cachedMask;
         try {
            cachedMask = maskCache != null ? maskCache.get(maskCacheScope, subject) : null;
         } catch (IllegalStateException e) {
            cachedMask = null;
         }
         if (cachedMask != null) {
            return cachedMask;
         } else {
            int mask = 0;
            PrincipalRoleMapper roleMapper = globalConfiguration.authorization().principalRoleMapper();
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
            try {
               if (maskCache != null) {
                  maskCache.put(maskCacheScope, subject, mask, globalConfiguration.securityCacheTimeout(), TimeUnit.MILLISECONDS);
               }
            } catch (IllegalStateException e) {
               // Ignore
            }
            return mask;
         }
      } else {
         return 0;
      }
   }
}
