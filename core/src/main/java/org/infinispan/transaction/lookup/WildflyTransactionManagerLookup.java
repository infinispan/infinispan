package org.infinispan.transaction.lookup;

import java.lang.reflect.Method;

import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Wildfly transaction client lookup (WildFly 11 and later).
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public class WildflyTransactionManagerLookup implements TransactionManagerLookup {
   private static final String WILDFLY_TM_CLASS_NAME = "org.wildfly.transaction.client.ContextTransactionManager";
   private static final String WILDFLY_UT_CLASS_NAME = "org.wildfly.transaction.client.LocalUserTransaction";
   private static final String WILDFLY_STATIC_METHOD = "getInstance";
   private static final Log log = LogFactory.getLog(WildflyTransactionManagerLookup.class);
   private Method manager, user;

   @Inject
   public void init(GlobalConfiguration globalCfg) {
      init(globalCfg.classLoader());
   }

   /**
    * @deprecated Use {@link #init(GlobalConfiguration)} instead since {@link Configuration} has no access to
    * classloader any more.
    */
   @Deprecated
   public void init(Configuration configuration) {
      init(new GlobalConfigurationBuilder().build());
   }

   @Override
   public synchronized TransactionManager getTransactionManager() throws Exception {
      TransactionManager tm = (TransactionManager) manager.invoke(null);
      if (log.isInfoEnabled()) {
         log.retrievingTm(tm);
      }
      return tm;
   }

   public UserTransaction getUserTransaction() throws Exception {
      return (UserTransaction) user.invoke(null);
   }

   @Override
   public String toString() {
      return "JBossStandaloneJTAManagerLookup";
   }

   private void init(ClassLoader classLoader) {
      // The TM may be deployed embedded alongside the app, so this needs to be looked up on the same CL as the Cache
      try {
         manager = Util.loadClassStrict(WILDFLY_TM_CLASS_NAME, classLoader).getMethod(WILDFLY_STATIC_METHOD);
         user = Util.loadClassStrict(WILDFLY_UT_CLASS_NAME, classLoader).getMethod(WILDFLY_STATIC_METHOD);
      } catch (SecurityException | NoSuchMethodException | ClassNotFoundException e) {
         throw new RuntimeException(e);
      }
   }
}
