package org.infinispan.security.impl;

import java.text.MessageFormat;

import javax.security.auth.Subject;

import org.infinispan.security.AuditContext;
import org.infinispan.security.AuditLogger;
import org.infinispan.security.AuditResponse;
import org.infinispan.security.AuthorizationPermission;
import org.jboss.logging.Logger;

/**
 * DefaultAuditLogger. A simple {@link AuditLogger} which send audit messages to a named
 * logger "AUDIT"
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public class DefaultAuditLogger implements AuditLogger {
   static final Logger auditLog = Logger.getLogger("AUDIT");
   static final MessageFormat format = new MessageFormat("[%s] %s %s %s[%s]"); // e.g. [ALLOW] user READ cache[defaultCache]

   @Override
   public void audit(Subject subject, AuditContext context, String contextName, AuthorizationPermission permission,
         AuditResponse response) {
      Object args[] = new Object[] { response, subject, permission, context, contextName };
      auditLog.trace(format.format(args));
   }
}
