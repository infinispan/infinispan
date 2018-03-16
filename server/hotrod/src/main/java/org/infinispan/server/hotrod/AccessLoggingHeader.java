package org.infinispan.server.hotrod;

import java.time.temporal.Temporal;

import javax.security.auth.Subject;

import org.infinispan.security.Security;

public class AccessLoggingHeader extends HotRodHeader {
   public final Object principalName;
   public final Object key;
   public final int requestBytes;
   public final Temporal requestStart;

   public AccessLoggingHeader(HotRodHeader header, Subject subject, Object key, int requestBytes, Temporal requestStart) {
      super(header);
      this.principalName = subject != null ? Security.getSubjectUserPrincipal(subject).getName() : null;
      this.key = key;
      this.requestBytes = requestBytes;
      this.requestStart = requestStart;
   }
}
