package org.infinispan.security.audit;

import javax.security.auth.Subject;

import org.infinispan.security.AuditContext;
import org.infinispan.security.AuditLogger;
import org.infinispan.security.AuditResponse;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.impl.AuditMessages;
import org.infinispan.telemetry.InfinispanSpanAttributes;
import org.infinispan.telemetry.InfinispanTelemetry;
import org.infinispan.telemetry.SpanCategory;
import org.jboss.logging.Logger;

/**
 * LoggingAuditLogger. A simple {@link AuditLogger} which send audit messages to a named
 * logger "org.infinispan.AUDIT"
 *
 * @author Tristan Tarrant
 * @since 7.0
 * @api.public
 */
public class LoggingAuditLogger implements AuditLogger {
   static final AuditMessages auditLog = Logger.getMessageLogger(AuditMessages.class, "org.infinispan.AUDIT");

   volatile InfinispanTelemetry telemetryService;

   public void setTelemetryService(InfinispanTelemetry telemetryService) {
      this.telemetryService = telemetryService;
   }

   @Override
   public void audit(Subject subject, AuditContext context, String contextName, AuthorizationPermission permission,
         AuditResponse response) {
      auditLog.auditMessage(response, Security.getSubjectUserPrincipal(subject), permission, context, contextName);

      if (telemetryService != null) {
         String cacheName = (AuditContext.CACHE.equals(context)) ? contextName : context.toString();

         InfinispanSpanAttributes attributes = new InfinispanSpanAttributes.Builder(SpanCategory.SECURITY)
               .withCacheName(cacheName)
               .build();

         telemetryService.startTraceRequest(response.name(), attributes).complete();
      }
   }

   @Override
   public boolean equals(Object obj) {
      return obj instanceof LoggingAuditLogger;
   }
}
