package org.infinispan.server.infinispan.spi;

import org.jboss.msc.service.ServiceName;

public class InfinispanSubsystem {
   public static final String SUBSYSTEM_NAME = "datagrid-infinispan";

   public static final ServiceName SERVICE_NAME_BASE = ServiceName.JBOSS.append(InfinispanSubsystem.SUBSYSTEM_NAME);
}
