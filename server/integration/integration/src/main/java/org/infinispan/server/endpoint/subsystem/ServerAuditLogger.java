package org.infinispan.server.endpoint.subsystem;

import javax.security.auth.Subject;

import org.infinispan.security.AuditContext;
import org.infinispan.security.AuditLogger;
import org.infinispan.security.AuditResponse;
import org.infinispan.security.AuthorizationPermission;
import org.jboss.security.audit.AuditEvent;
import org.jboss.security.audit.AuditLevel;
import org.jboss.security.audit.AuditManager;

/**
 * ServerAuditLogger.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class ServerAuditLogger implements AuditLogger {

   private final AuditManager auditManager;

   ServerAuditLogger(AuditManager auditManager) {
      this.auditManager = auditManager;
   }

   @Override
   public void audit(Subject subject, AuditContext context, String contextName, AuthorizationPermission permission, AuditResponse response) {
      String level;
      switch (response) {
      case ALLOW:
         level = AuditLevel.SUCCESS;
         break;
      case DENY:
         level = AuditLevel.FAILURE;
         break;
      case ERROR:
         level = AuditLevel.ERROR;
         break;
      default:
         level = AuditLevel.INFO;
         break;
      }
      AuditEvent ae = new AuditEvent(level);
      auditManager.audit(ae);
   }

}
