package org.infinispan.security.impl;

import static org.infinispan.util.logging.Log.SECURITY;

import java.security.AccessControlException;
import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import javax.security.auth.Subject;

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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

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
   private final ConcurrentMap<CachePrincipalPair, SubjectACL> aclCache;

   public AuthorizationHelper(GlobalSecurityConfiguration globalConfiguration, AuditContext context, String name,
         ConcurrentMap<CachePrincipalPair, SubjectACL> aclCache) {
      this.globalConfiguration = globalConfiguration;
      this.audit = globalConfiguration.authorization().auditLogger();
      this.context = context;
      this.name = name;
      this.aclCache = aclCache;
   }

   public AuthorizationHelper(GlobalSecurityConfiguration security, AuditContext context, String name) {
      this(security, context, name, security.authorization().enabled() ? createAclCache() : null);
   }

   private static <K, V> ConcurrentMap<K, V> createAclCache() {
      Cache<K, V> cache = Caffeine.newBuilder().maximumSize(1000).build();
      return cache.asMap();
   }

   public void checkPermission(AuthorizationPermission perm) {
      checkPermission(null, null, perm, null);
   }

   public void checkPermission(AuthorizationPermission perm, String role) {
      checkPermission(null, null, perm, role);
   }

   public void checkPermission(AuthorizationConfiguration configuration, AuthorizationPermission perm) {
      checkPermission(configuration, null, perm, null);
   }

   public void checkPermission(Subject subject, AuthorizationPermission perm) {
      checkPermission(null, subject, perm, null);
   }

   public SubjectACL getACL(Subject subject) {
      return getACL(subject, null);
   }

   public SubjectACL getACL(Subject subject, AuthorizationConfiguration configuration) {
      if (globalConfiguration.authorization().enabled()) {
         return computeSubjectACL(subject, configuration);
      } else {
         return new SubjectACL(Collections.emptySet(), AuthorizationPermission.ALL.getMask());
      }
   }

   public void checkPermission(AuthorizationConfiguration configuration, Subject subject, AuthorizationPermission perm,
         String role) {
      if (globalConfiguration.authorization().enabled()) {
         if (Security.isPrivileged()) {
            Security.checkPermission(perm.getSecurityPermission());
         } else {
            subject = subject != null ? subject : Security.getSubject();
            try {
               if (subject != null) {
                  if (checkSubjectPermissionAndRole(subject, configuration, perm, role)) {
                     audit.audit(subject, context, name, perm, AuditResponse.ALLOW);
                  } else {
                     checkSecurityManagerPermission(perm);
                  }
               } else {
                  checkSecurityManagerPermission(perm);
               }
            } catch (SecurityException e) {
               audit.audit(subject, context, name, perm, AuditResponse.DENY);
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
      Principal userPrincipal = Security.getSubjectUserPrincipal(subject);
      if (userPrincipal != null) {
         CachePrincipalPair cpp = new CachePrincipalPair(userPrincipal, name);
         SubjectACL subjectACL;
         if (aclCache != null)
            subjectACL = aclCache.computeIfAbsent(cpp, s -> computeSubjectACL(subject, configuration));
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
      PrincipalRoleMapper roleMapper = globalConfiguration.authorization().principalRoleMapper();
      Set<Principal> principals = subject.getPrincipals();
      Set<String> allRoles = new HashSet<>(principals.size());
      for (Principal principal : principals) {
         Set<String> roleNames = roleMapper.principalToRoles(principal);
         if (roleNames != null) {
            allRoles.addAll(roleNames);
         }
      }
      int subjectMask = 0;
      Map<String, Role> globalRoles = globalConfiguration.authorization().roles();
      for (String role : allRoles) {
         if (configuration == null || configuration.roles().isEmpty() || configuration.roles().contains(role)) {
            Role globalRole = globalRoles.get(role);
            if (globalRole != null) {
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
