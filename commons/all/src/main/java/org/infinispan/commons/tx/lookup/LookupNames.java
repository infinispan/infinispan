package org.infinispan.commons.tx.lookup;

import static org.infinispan.commons.logging.Log.CONTAINER;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.TransactionManager;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;


/**
 * The JDNI and {@link TransactionManager} factories we know of.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public final class LookupNames {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private LookupNames() {
   }

   /**
    * JNDI locations for TransactionManagers we know of.
    */
   public enum JndiTransactionManager {
      JBOSS_AS_7("java:jboss/TransactionManager", "JBoss AS 7"),
      JBOSS_AS_4_6("java:/TransactionManager", "JBoss AS 4 ~ 6, JRun4"),
      RESIN_3("java:comp/TransactionManager", "Resin 3.x"),
      GLASSFISH("java:appserver/TransactionManager", "Sun Glassfish"),
      BORLAND_SUN("java:pm/TransactionManager", "Borland, Sun"),
      WEBLOGIC("javax.transaction.TransactionManager", "BEA WebLogic"),
      RESIN_ORION_JONAS("java:comp/UserTransaction", "Resin, Orion, JOnAS (JOTM)");

      private final String jndiLookup;
      private final String prettyName;

      JndiTransactionManager(String jndiLookup, String prettyName) {
         this.jndiLookup = jndiLookup;
         this.prettyName = prettyName;
      }

      public String getJndiLookup() {
         return jndiLookup;
      }

      public String getPrettyName() {
         return prettyName;
      }
   }

   /**
    * TransactionManager factories we know of.
    */
   public enum TransactionManagerFactory {
      WEBSPHERE_LIBERTY("com.ibm.tx.jta.TransactionManagerFactory", "WebSphere Liberty", "getTransactionManager"),
      WEBSPHERE_51_6("com.ibm.ws.Transaction.TransactionManagerFactory", "WebSphere 5.1 and 6.0",
            "getTransactionManager"),
      WEBSPHERE_6("com.ibm.ejs.jts.jta.TransactionManagerFactory", "WebSphere 5.0", "getTransactionManager"),
      WEBSPHERE_4("com.ibm.ejs.jts.jta.JTSXA", "WebSphere 4.0", "getTransactionManager"),
      WILDFLY("org.wildfly.transaction.client.ContextTransactionManager", "Wildfly 11 and later", "getInstance"),
      JBOSS_TM("com.arjuna.ats.jta.TransactionManager", "JBoss Standalone TM", "transactionManager");

      private final String factoryClazz;
      private final String prettyName;
      private final String factoryMethod;

      TransactionManagerFactory(String factoryClazz, String prettyName, String factoryMethod) {
         this.factoryClazz = factoryClazz;
         this.prettyName = prettyName;
         this.factoryMethod = factoryMethod;
      }

      public String getFactoryClazz() {
         return factoryClazz;
      }

      public String getPrettyName() {
         return prettyName;
      }

      public TransactionManager tryLookup(ClassLoader classLoader) {
         Class<?> clazz;
         try {
            clazz = Util.loadClassStrict(factoryClazz, classLoader);
            Method method = clazz.getMethod(factoryMethod);
            return (TransactionManager) method.invoke(null);
         } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException ignored) {
            return null;
         }
      }
   }

   /**
    * Lookup for {@link TransactionManager}.
    * <p>
    * It looks up by JNDI first using {@link #lookupByJNDI()} and then by {@link TransactionManager} factories, using
    * {@link #lookupByTransactionManagerFactory(ClassLoader)}.
    *
    * @param classLoader The {@link ClassLoader} to be used by {@link #lookupByTransactionManagerFactory(ClassLoader)}
    * @return An {@link Optional} with the {@link TransactionManager} if found.
    */
   public static Optional<TransactionManager> lookupKnownTransactionManagers(ClassLoader classLoader) {
      return lookupByJNDI()
            .or(() -> lookupByTransactionManagerFactory(classLoader));
   }

   /**
    * Lookup for a {@link TransactionManager} by known JNDI names.
    *
    * @return An {@link Optional} with the {@link TransactionManager} if found.
    */
   public static Optional<TransactionManager> lookupByJNDI() {
      InitialContext ctx;
      try {
         ctx = new InitialContext();
      } catch (NamingException e) {
         CONTAINER.failedToCreateInitialCtx(e);
         return Optional.empty();
      }

      try {
         for (var lookup : JndiTransactionManager.values()) {
            Object jndiObject;
            try {
               log.debugf("Trying to lookup TransactionManager for %s", lookup.getPrettyName());
               jndiObject = ctx.lookup(lookup.getJndiLookup());
            } catch (NamingException e) {
               log.debugf("Failed to perform a lookup for [%s (%s)]",
                     lookup.getJndiLookup(), lookup.getPrettyName());
               continue;
            }
            if (jndiObject instanceof TransactionManager) {
               log.debugf("Found TransactionManager for %s", lookup.getPrettyName());
               return Optional.of((TransactionManager) jndiObject);
            }
         }
      } finally {
         Util.close(ctx);
      }
      return Optional.empty();
   }

   /**
    * Lookup a {@link TransactionManager} by factory.
    *
    * @param classLoader The {@link ClassLoader} to use to instantiate the factory.
    * @return An {@link Optional} with the {@link TransactionManager} if found.
    */
   public static Optional<TransactionManager> lookupByTransactionManagerFactory(ClassLoader classLoader) {
      for (var factory : TransactionManagerFactory.values()) {
         log.debugf("Trying %s: %s", factory.getPrettyName(), factory.getFactoryClazz());
         TransactionManager transactionManager = factory.tryLookup(classLoader);
         if (transactionManager != null) {
            log.debugf("Found %s: %s", factory.getPrettyName(), factory.getFactoryClazz());
            return Optional.of(transactionManager);
         }
      }
      return Optional.empty();
   }


}
