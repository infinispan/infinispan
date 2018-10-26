package org.infinispan.marshall.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * As much as possible, Infinispan consumers should provide
 * {@link org.infinispan.commons.marshall.Externalizer} or
 * {@link org.infinispan.commons.marshall.AdvancedExternalizer} instances
 * for the types being marshalled, so that these types can be marshalled
 * as efficiently as possible.
 *
 * Sometimes however, Infinispan consumers might rely on the fact
 * that a certain type implements Java's standard {@link Serializable}
 * or {@link java.io.Externalizable}.
 *
 * This class acts a test barrier which controls, provided assertions
 * have been enabled, which types can be externally marshalled using
 * JBoss Marshalling.
 *
 * The plan is for external marshalling is be morphed into user type
 * marshalling, at which point this class won't be used any more.
 *
 * @since 9.0
 */
public final class ExternallyMarshallable {

   private static final List<String> whiteListClasses = new ArrayList<>();

   static {
      whiteListClasses.add("Exception");
      whiteListClasses.add("$$Lambda$");
      whiteListClasses.add("java.lang.Class");
      whiteListClasses.add("java.time.Instant"); // prod
      whiteListClasses.add("org.hibernate.search.query.engine.impl.LuceneHSQuery"); // prod
      whiteListClasses.add("org.infinispan.distexec.RunnableAdapter"); // prod
      whiteListClasses.add("org.infinispan.jcache.annotation.DefaultCacheKey"); // prod
      whiteListClasses.add("org.infinispan.server.core.transport.NettyTransportConnectionStats$ConnectionAdderTask"); // prod
      whiteListClasses.add("org.infinispan.server.hotrod.CheckAddressTask"); // prod
      whiteListClasses.add("org.infinispan.server.infinispan.task.DistributedServerTask"); // prod
      whiteListClasses.add("org.infinispan.scripting.impl.DataType"); // prod
      whiteListClasses.add("org.infinispan.scripting.impl.DistributedScript");
      whiteListClasses.add("org.infinispan.stats.impl.ClusterCacheStatsImpl$DistributedCacheStatsCallable"); // prod
      whiteListClasses.add("org.infinispan.xsite.BackupSender$TakeSiteOfflineResponse"); // prod
      whiteListClasses.add("org.infinispan.xsite.BackupSender$BringSiteOnlineResponse"); // prod
      whiteListClasses.add("org.infinispan.xsite.XSiteAdminCommand$Status"); // prod
      whiteListClasses.add("org.infinispan.util.logging.events.EventLogLevel"); // prod
      whiteListClasses.add("org.infinispan.util.logging.events.EventLogCategory"); // prod
      whiteListClasses.add("java.util.Date"); // test
      whiteListClasses.add("java.lang.Byte"); // test
      whiteListClasses.add("java.lang.Integer"); // test
      whiteListClasses.add("java.lang.Double"); // test
      whiteListClasses.add("java.lang.Short"); // test
      whiteListClasses.add("java.lang.Long"); // test
      whiteListClasses.add("org.infinispan.test"); // test
      whiteListClasses.add("org.infinispan.server.test"); // test
      whiteListClasses.add("org.infinispan.it"); // test
      whiteListClasses.add("org.infinispan.all"); // test
      whiteListClasses.add("org.infinispan.query.api"); // test
      whiteListClasses.add("org.infinispan.stream.BaseStreamTest$TestClass"); // test
      whiteListClasses.add("org.jboss.as.quickstarts.datagrid"); // quickstarts testing
   }

   private ExternallyMarshallable() {
      // Static class
   }

   /**
    * Adds package or class name to the externally marshallable white list.
    */
   public static void addToWhiteList(String type) {
      whiteListClasses.add(type);
   }

   public static boolean isAllowed(Object obj) {
      Class<?> clazz = obj.getClass();
      return isAllowed(clazz);
   }

   public static boolean isAllowed(Class<?> clazz) {
      Package pkg = clazz.getPackage();
      if (pkg == null) {
         if (clazz.isArray()) {
            return true;
         } else {
            throw new IllegalStateException("No package info for " + clazz + ", runtime-generated class?");
         }
      }
      String pkgName = pkg.getName();
      boolean isBlackList =
            Serializable.class.isAssignableFrom(clazz)
            && isMarshallablePackage(pkgName)
            && !ExternalPojo.class.isAssignableFrom(clazz)
            && !isWhiteList(clazz.getName());
      return !isBlackList;
   }

   private static boolean isMarshallablePackage(String pkg) {
      return pkg.startsWith("java.")
            || pkg.startsWith("org.infinispan.")
            || pkg.startsWith("org.jgroups.")
            || pkg.startsWith("org.apache")
            || pkg.startsWith("org.jboss")
            || pkg.startsWith("com.arjuna")
            ;
   }

   private static boolean isWhiteList(String className) {
      return whiteListClasses.stream().anyMatch(className::contains);
   }

}
