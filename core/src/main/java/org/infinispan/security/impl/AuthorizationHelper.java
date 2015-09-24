package org.infinispan.security.impl;

import java.security.AccessControlException;
import java.security.Principal;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.security.auth.Subject;

import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AuthorizationConfiguration;
import org.infinispan.configuration.global.GlobalSecurityConfiguration;
import org.infinispan.security.AuditContext;
import org.infinispan.security.AuditLogger;
import org.infinispan.security.AuditResponse;
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
   private final ConcurrentMap<Subject, SubjectACL> aclCache;

   public AuthorizationHelper(GlobalSecurityConfiguration globalConfiguration, AuditContext context, String name, ConcurrentMap<Subject, SubjectACL> cache) {
      this.globalConfiguration = globalConfiguration;
      this.audit = globalConfiguration.authorization().auditLogger();
      this.context = context;
      this.name = name;
      this.aclCache = cache;
   }

   public AuthorizationHelper(GlobalSecurityConfiguration security, AuditContext context, String name) {
      this(security, context, name, CollectionFactory.makeBoundedConcurrentMap(10));
   }

   public void checkPermission(AuthorizationPermission perm) {
      checkPermission(null, perm, Optional.empty());
   }

   public void checkPermission(AuthorizationConfiguration configuration, AuthorizationPermission perm) {
      checkPermission(configuration, perm, Optional.empty());
   }

   public void checkPermission(AuthorizationConfiguration configuration, AuthorizationPermission perm, Optional<String> role) {
      if (globalConfiguration.authorization().enabled()) {
         if (Security.isPrivileged()) {
            Security.checkPermission(perm.getSecurityPermission());
         } else {
            Subject subject = Security.getSubject();
            try {
               if (subject != null) {
                  if(checkSubjectPermissionAndRole(subject, configuration, perm, role)) {
                     audit.audit(subject, context, name, perm, AuditResponse.ALLOW);
                  } else {
                     checkSecurityManagerPermission(perm);
                  }
               } else {
                  checkSecurityManagerPermission(perm);
               }
            } catch (SecurityException e) {
               audit.audit(subject, context, name, perm, AuditResponse.DENY);
               throw log.unauthorizedAccess(Util.prettyPrintSubject(subject), perm.toString());
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

   private boolean checkSubjectPermissionAndRole(Subject subject, AuthorizationConfiguration configuration,
         AuthorizationPermission requiredPermission, Optional<String> requestedRole) {
      if (subject != null) {
         SubjectACL subjectACL = aclCache.computeIfAbsent(subject, s -> {
            PrincipalRoleMapper roleMapper = globalConfiguration.authorization().principalRoleMapper();
            Set<String> allRoles = new HashSet<>();
            for (Principal principal : subject.getPrincipals()) {
               Set<String> roleNames = roleMapper.principalToRoles(principal);
               if (roleNames != null) {
                  allRoles.addAll(roleNames);
               }
            }
            return new SubjectACL(allRoles);
         });
         int subjectMask = 0;
         Map<String, Role> globalRoles = globalConfiguration.authorization().roles();
         for(String role : subjectACL.roles) {
            if (configuration == null || configuration.roles().contains(role)) {
               Role globalRole = globalRoles.get(role);
               if (globalRole != null)
                  subjectMask |= globalRole.getMask();
            }
         }
         int permissionMask = requiredPermission.getMask();
         return (subjectMask & permissionMask) == permissionMask && requestedRole.map(r -> subjectACL.roles.contains(r)).orElse(true);
      } else {
         return false;
      }
   }
}
