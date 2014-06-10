package org.infinispan.security.impl;

import java.security.Principal;

import org.infinispan.security.AuditContext;
import org.infinispan.security.AuditResponse;
import org.infinispan.security.AuthorizationPermission;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * AuditMessages. Messages used by the security audit
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
@MessageLogger(projectCode="")
public interface AuditMessages {

   @LogMessage
   @Message("[%s] %s %s %s[%s]")
   void auditMessage(AuditResponse response, Principal principal, AuthorizationPermission permission, AuditContext context, String contextName);
}
