package org.infinispan.security.impl;

import javax.security.auth.Subject;

import org.infinispan.security.AuditContext;
import org.infinispan.security.AuditLogger;
import org.infinispan.security.AuditResponse;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.jboss.logging.Logger;

/**
 * LoggingAuditLogger. A simple {@link AuditLogger} which send audit messages to a named
 * logger "org.infinispan.AUDIT"
 *
 * @author Tristan Tarrant
 * @since 7.0
 * @public
 */
public class LoggingAuditLogger implements AuditLogger {
   static final AuditMessages auditLog = Logger.getMessageLogger(AuditMessages.class, "org.infinispan.AUDIT");

   @Override
   public void audit(Subject subject, AuditContext context, String contextName, AuthorizationPermission permission,
         AuditResponse response) {
      auditLog.auditMessage(response, Security.getSubjectUserPrincipal(subject), permission, context, contextName);
   }
}
