package org.infinispan.commons.tx.lookup;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.transaction.TransactionManager;

import org.infinispan.commons.util.Util;

/**
 * The JDNI and {@link TransactionManager} factories we know of.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public final class LookupNames {

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
      RESIN_ORION_JONAS("java:comp/UserTransaction", "Resin, Orion, JOnAS (JOTM)"),
      KARAF("osgi:service/javax.transaction.TransactionManager", "Karaf");

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


}
