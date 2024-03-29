package org.infinispan.transaction.lookup;

import java.lang.reflect.Method;

import jakarta.transaction.TransactionManager;
import jakarta.transaction.UserTransaction;

import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * JTA standalone TM lookup (JBoss AS 7 and earlier, and WildFly 8, 9, and 10).
 *
 * @author Jason T. Greene
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public class JBossStandaloneJTAManagerLookup implements TransactionManagerLookup {
   private Method manager, user;
   private static final Log log = LogFactory.getLog(JBossStandaloneJTAManagerLookup.class);

   @Inject
   public void init(GlobalConfiguration globalCfg) {
      init(globalCfg.classLoader());
   }

   private void init(ClassLoader classLoader) {
      // The TM may be deployed embedded alongside the app, so this needs to be looked up on the same CL as the Cache
      try {
         manager = Util.loadClass("com.arjuna.ats.jta.TransactionManager", classLoader).getMethod("transactionManager");
         user = Util.loadClass("com.arjuna.ats.jta.UserTransaction", classLoader).getMethod("userTransaction");
      } catch (SecurityException | NoSuchMethodException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public synchronized TransactionManager getTransactionManager() throws Exception {
      TransactionManager tm = (TransactionManager) manager.invoke(null);
      if (log.isInfoEnabled()) log.retrievingTm(tm);
      return tm;
   }

   public UserTransaction getUserTransaction() throws Exception {
      return (UserTransaction) user.invoke(null);
   }

   @Override
   public String toString() {
      return "JBossStandaloneJTAManagerLookup";
   }
}
