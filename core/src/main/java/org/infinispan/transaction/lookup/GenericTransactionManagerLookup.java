package org.infinispan.transaction.lookup;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.transaction.TransactionManager;

import org.infinispan.commons.tx.lookup.LookupNames;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A transaction manager lookup class that attempts to locate a TransactionManager. A variety of different classes and
 * JNDI locations are tried, for servers such as: <ul> <li> JBoss <li> JRun4 <li> Resin <li> Orion <li> JOnAS <li> BEA
 * Weblogic <li> Websphere 4.0, 5.0, 5.1, 6.0 <li> Sun, Glassfish </ul> If a transaction manager is not found, returns
 * an {@link org.infinispan.transaction.tm.EmbeddedTransactionManager}.
 *
 * @author Markus Plesser
 * @since 4.0
 */
public class GenericTransactionManagerLookup implements TransactionManagerLookup {

   private static final Log log = LogFactory.getLog(GenericTransactionManagerLookup.class);

   public static final GenericTransactionManagerLookup INSTANCE = new GenericTransactionManagerLookup();

   /**
    * JNDI lookups performed?
    */
   private boolean lookupDone = false;

   /**
    * No JNDI available?
    */
   private boolean lookupFailed = false;

   /**
    * No JBoss TM embedded jars found?
    */
   private boolean noJBossTM = false;

   /**
    * The JTA TransactionManager found.
    */
   private TransactionManager tm = null;

   @Inject private GlobalConfiguration globalCfg;

   /**
    * Get the system-wide used TransactionManager
    *
    * @return TransactionManager
    */
   @Override
   public synchronized TransactionManager getTransactionManager() {
      if (!lookupDone) {
         doLookups(globalCfg.classLoader());
      }
      if (tm != null)
         return tm;
      if (lookupFailed) {
         if (!noJBossTM) {
            // First try an embedded JBossTM/Wildly instance
            tryEmbeddedJBossTM();
         }

         if (noJBossTM) {
            //fall back to a dummy from Infinispan
            useDummyTM();
         }
      }
      return tm;
   }

   private void useDummyTM() {
      tm = EmbeddedTransactionManager.getInstance();
      log.fallingBackToEmbeddedTm();
   }

   private void tryEmbeddedJBossTM() {
      try {
         WildflyTransactionManagerLookup lookup = new WildflyTransactionManagerLookup();
         lookup.init(globalCfg);
         tm = lookup.getTransactionManager();
         return;
      } catch (Exception e) {
         //ignore. lets try JBossStandalone
      }
      try {
         JBossStandaloneJTAManagerLookup jBossStandaloneJTAManagerLookup = new JBossStandaloneJTAManagerLookup();
         jBossStandaloneJTAManagerLookup.init(globalCfg);
         tm = jBossStandaloneJTAManagerLookup.getTransactionManager();
      } catch (Exception e) {
         noJBossTM = true;
      }
   }

   /**
    * Try to figure out which TransactionManager to use
    */
   private void doLookups(ClassLoader cl) {
      if (lookupFailed)
         return;
      InitialContext ctx;
      try {
         ctx = new InitialContext();
      }
      catch (NamingException e) {
         log.failedToCreateInitialCtx(e);
         lookupFailed = true;
         return;
      }

      try {
         //probe jndi lookups first
         for (LookupNames.JndiTransactionManager knownJNDIManager : LookupNames.JndiTransactionManager.values()) {
            Object jndiObject;
            try {
               log.debugf("Trying to lookup TransactionManager for %s", knownJNDIManager.getPrettyName());
               jndiObject = ctx.lookup(knownJNDIManager.getJndiLookup());
            }
            catch (NamingException e) {
               log.debugf("Failed to perform a lookup for [%s (%s)]",
                         knownJNDIManager.getJndiLookup(), knownJNDIManager.getPrettyName());
               continue;
            }
            if (jndiObject instanceof TransactionManager) {
               tm = (TransactionManager) jndiObject;
               log.debugf("Found TransactionManager for %s", knownJNDIManager.getPrettyName());
               return;
            }
         }
      } finally {
         Util.close(ctx);
      }

      boolean found = true;
      // The TM may be deployed embedded alongside the app, so this needs to be looked up on the same CL as the Cache
      for (LookupNames.TransactionManagerFactory transactionManagerFactory : LookupNames.TransactionManagerFactory.values()) {
         log.debugf("Trying %s: %s", transactionManagerFactory.getPrettyName(), transactionManagerFactory.getFactoryClazz());
         TransactionManager transactionManager = transactionManagerFactory.tryLookup(cl);
         if (transactionManager != null) {
            log.debugf("Found %s: %s", transactionManagerFactory.getPrettyName(), transactionManagerFactory.getFactoryClazz());
            tm = transactionManager;
            found = false;
         }
      }
      lookupDone = true;
      lookupFailed = found;
   }

}
