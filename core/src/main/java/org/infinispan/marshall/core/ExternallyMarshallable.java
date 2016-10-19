package org.infinispan.marshall.core;

import java.io.Serializable;

final class ExternallyMarshallable {

   private ExternallyMarshallable() {
      // Static class
   }

   static boolean isAllowed(Object obj) {
      String pkg = obj.getClass().getPackage().getName();
      boolean isBlackList =
            obj instanceof Serializable
            && isMarshallablePackage(pkg)
            && !(obj instanceof ExternalPojo)
            && !isWhiteList(obj.getClass().getName());
      return !isBlackList;
   }

   private static boolean isMarshallablePackage(String pkg) {
      return pkg.startsWith("java.")
            || pkg.startsWith("org.infinispan.")
            || pkg.startsWith("org.jgroups.")
            || pkg.startsWith("org.hibernate")
            || pkg.startsWith("org.apache")
            || pkg.startsWith("org.jboss")
            || pkg.startsWith("com.arjuna")
            ;
   }

   private static boolean isWhiteList(String className) {
      return className.endsWith("Exception")
            || className.contains("$$Lambda$")
            || className.equals("java.lang.Class")
            || className.equals("java.time.Instant") // prod
            || className.equals("org.hibernate.search.query.engine.impl.LuceneHSQuery") // prod
            || className.equals("org.infinispan.distexec.DefaultExecutorService$RunnableAdapter") // prod
            || className.equals("org.infinispan.jcache.annotation.DefaultCacheKey") // prod
            || className.equals("org.infinispan.query.clustered.QueryResponse") // prod
            || className.equals("org.infinispan.server.core.transport.NettyTransport$ConnectionAdderTask") // prod
            || className.equals("org.infinispan.server.hotrod.CheckAddressTask") // prod
            || className.equals("org.infinispan.server.infinispan.task.DistributedServerTask") // prod
            || className.equals("org.infinispan.scripting.impl.DataType") // prod
            || className.equals("org.infinispan.scripting.impl.DistributedScript")
            || className.equals("org.infinispan.stats.impl.ClusterCacheStatsImpl$DistributedCacheStatsCallable") // prod
            || className.equals("org.infinispan.xsite.BackupSender$TakeSiteOfflineResponse") // prod
            || className.equals("org.infinispan.xsite.BackupSender$BringSiteOnlineResponse") // prod
            || className.equals("org.infinispan.xsite.XSiteAdminCommand$Status") // prod
            || className.equals("org.infinispan.util.logging.events.EventLogLevel") // prod
            || className.equals("org.infinispan.util.logging.events.EventLogCategory") // prod
            || className.equals("java.util.Date") // test
            || className.startsWith("org.infinispan.test") // test
            || className.startsWith("org.infinispan.server.test") // test
            || className.startsWith("org.infinispan.it") // test
            || className.startsWith("org.infinispan.all") // test
            ;
   }

}
