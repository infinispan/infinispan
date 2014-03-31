package org.infinispan.security;

import javax.security.auth.Subject;

/**
 * AuditLogger.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public interface AuditLogger {
   public void audit(Subject subject, AuditContext context, String contextName, AuthorizationPermission permission, AuditResponse response);
}
