package org.infinispan.security.impl;

import static org.infinispan.util.logging.Log.SECURITY;

import java.security.AccessControlException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.AuthorizationConfiguration;
import org.infinispan.configuration.global.GlobalAuthorizationConfiguration;
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
 * Authorizer. Some utility methods for computing access masks and verifying them against permissions
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class Authorizer {
   private static final Log log = LogFactory.getLog(Authorizer.class);
   public static final SubjectACL SUPERUSER = new SubjectACL(Collections.emptySet(), AuthorizationPermission.ALL.getMask());
   private final GlobalSecurityConfiguration globalConfiguration;
   private final AuditLogger audit;
   private final AuditContext context;
   private final String name;
   private Map<CacheSubjectPair, SubjectACL> aclCache;

   public Authorizer(GlobalSecurityConfiguration globalConfiguration, AuditContext context, String name, Map<CacheSubjectPair, SubjectACL> aclCache) {
      this.globalConfiguration = globalConfiguration;
      this.audit = globalConfiguration.authorization().auditLogger();
      this.context = context;
      this.name = name;
      this.aclCache = aclCache;
   }

   public void setAclCache(Map<CacheSubjectPair, SubjectACL> aclCache) {
      this.aclCache = aclCache;
   }

   public void checkPermission(AuthorizationPermission perm) {
      checkPermission(null, null, name, context, null, perm);
   }

   public void checkPermission(AuthorizationPermission perm, String role) {
      checkPermission(null, null, name, context, role, perm);
   }

   public void checkPermission(AuthorizationConfiguration configuration, AuthorizationPermission perm) {
      checkPermission(configuration, null, name, context, null, perm);
   }

   public void checkPermission(Subject subject, AuthorizationPermission perm) {
      checkPermission(null, subject, name, context, null, perm);
   }

   public SubjectACL getACL(Subject subject) {
      return getACL(subject, null);
   }

   public SubjectACL getACL(Subject subject, AuthorizationConfiguration configuration) {
      if (globalConfiguration.authorization().enabled() && (configuration == null || configuration.enabled())) {
         return computeSubjectACL(subject, configuration);
      } else {
         return SUPERUSER;
      }
   }

   public void checkPermission(AuthorizationConfiguration configuration, Subject subject, AuthorizationPermission perm, String role) {
      checkPermission(configuration, subject, null, context, role, perm);
   }

   public void checkPermission(Subject subject, AuthorizationPermission perm, AuditContext explicitContext) {
      checkPermission(null, subject, null, explicitContext, null, perm);
   }

   public void checkPermission(Subject subject, AuthorizationPermission perm, String contextName, AuditContext auditContext) {
      checkPermission(null, subject, contextName, auditContext, null, perm);
   }

   public void checkPermission(AuthorizationConfiguration configuration, Subject subject, String explicitName, AuditContext explicitContext, String role, AuthorizationPermission perm) {
      if (globalConfiguration.authorization().enabled()) {
         if (Security.isPrivileged()) {
            Security.checkPermission(perm.getSecurityPermission());
         } else {
            subject = subject != null ? subject : Security.getSubject();
            try {
               if (subject != null) {
                  if (checkSubjectPermissionAndRole(subject, configuration, perm, role)) {
                     audit.audit(subject, explicitContext, explicitName, perm, AuditResponse.ALLOW);
                  } else {
                     checkSecurityManagerPermission(perm);
                  }
               } else {
                  checkSecurityManagerPermission(perm);
               }
            } catch (SecurityException e) {
               audit.audit(subject, explicitContext, explicitName, perm, AuditResponse.DENY);
               throw SECURITY.unauthorizedAccess(Util.prettyPrintSubject(subject), perm.toString());
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
                                                 AuthorizationPermission requiredPermission, String requestedRole) {
      if (subject != null) {
         CacheSubjectPair csp = new CacheSubjectPair(subject, name);
         SubjectACL subjectACL;
         if (aclCache != null)
            subjectACL = aclCache.computeIfAbsent(csp, s -> computeSubjectACL(subject, configuration));
         else
            subjectACL = computeSubjectACL(subject, configuration);

         int permissionMask = requiredPermission.getMask();
         boolean authorized = subjectACL.matches(permissionMask) && (requestedRole != null ? subjectACL.containsRole(requestedRole) : true);
         if (log.isTraceEnabled()) {
            log.tracef("Check subject '%s' with ACL '%s' has permission '%s' and role '%s' = %b", subject, subjectACL, requiredPermission, requestedRole, authorized);
         }
         return authorized;
      } else {
         return false;
      }
   }

   private SubjectACL computeSubjectACL(Subject subject, AuthorizationConfiguration configuration) {
      GlobalAuthorizationConfiguration authorization = globalConfiguration.authorization();
      PrincipalRoleMapper roleMapper = authorization.principalRoleMapper();
      Set<Principal> principals = subject.getPrincipals();
      Set<String> allRoles = new HashSet<>(principals.size());
      // Map all the Subject's principals to roles using the role mapper. There may be more than one role per principal
      for (Principal principal : principals) {
         Set<String> roleNames = roleMapper.principalToRoles(principal);
         if (roleNames != null) {
            allRoles.addAll(roleNames);
         }
      }
      // Create a bitmask of the permissions this Subject has for the resource identified by the configuration
      int subjectMask = 0;
      // If this resource has not declared any roles, all the inheritable global roles will be checked
      boolean implicit = configuration != null ? configuration.roles().isEmpty() : false;
      for (String role : allRoles) {
         if (configuration == null || implicit || configuration.roles().contains(role)) {
            Role globalRole = authorization.getRole(role);
            if (globalRole != null && (!implicit || globalRole.isInheritable())) {
               subjectMask |= globalRole.getMask();
            }
         }
      }
      if (log.isTraceEnabled()) {
         log.tracef("Subject '%s' has roles '%s' and permission mask %d", subject, allRoles, subjectMask);
      }
      return new SubjectACL(allRoles, subjectMask);
   }
}
