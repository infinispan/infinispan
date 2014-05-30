package org.infinispan.util.tx.lookup;

import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

import org.apache.geronimo.transaction.GeronimoUserTransaction;
import org.apache.geronimo.transaction.log.HOWLLog;
import org.apache.geronimo.transaction.manager.GeronimoTransactionManager;
import org.apache.geronimo.transaction.manager.TransactionLog;
import org.apache.geronimo.transaction.manager.XidFactoryImpl;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Transaction manager lookup for testing purposes. Use this lookup class to verify that
 * Infinispan works correctly with Geronimo Transaction Manager.
 * 
 * In order to use this TM lookup, run tests with -Dinfinispan.test.jta.tm=geronimotm
 * 
 * @author mgencur
 *
 */
public class GeronimoTransactionManagerLookup implements TransactionManagerLookup {

   private static final int DEFAULT_TRANSACTION_TIMEOUT = 600;
   private static final Log log = LogFactory.getLog(GeronimoTransactionManagerLookup.class);
   
   private TransactionManager manager;
   private UserTransaction userTransaction;
   private TransactionLog transactionLog;

   @Inject
   public void init(GlobalConfiguration globalCfg) {
      final String bufferClassName = "org.objectweb.howl.log.BlockLogBuffer";
      final int bufferSizeKBytes = 1;
      final boolean checksumEnabled = true;
      final boolean adler32Checksum = true;
      final int flushSleepTimeMilliseconds = 50;
      final String logFileExt = "log";
      final String logFileName = "transaction";
      final int maxBlocksPerFile = -1;
      final int maxLogFiles = 2;
      final int minBuffers = 4;
      final int maxBuffers = 0;
      final int threadsWaitingForceThreshold = -1;
      final String logFileDir = System.getProperty("java.io.tmpdir");
      try {
         transactionLog = new HOWLLog(bufferClassName, bufferSizeKBytes, checksumEnabled, adler32Checksum,
               flushSleepTimeMilliseconds, logFileDir, logFileExt, logFileName, maxBlocksPerFile, maxBuffers,
               maxLogFiles, minBuffers, threadsWaitingForceThreshold, new XidFactoryImpl(), null);
         ((HOWLLog) transactionLog).doStart();
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public synchronized TransactionManager getTransactionManager() throws Exception {
      if (manager == null) {
         try {
            manager = new GeronimoTransactionManager(DEFAULT_TRANSACTION_TIMEOUT, transactionLog);
         } catch (Exception e) {
            throw new RuntimeException(e);
         }
      }
      if (log.isInfoEnabled()) {
         log.retrievingTm(manager);
      }
      return manager;
   }

   public UserTransaction getUserTransaction() throws Exception {
      if (userTransaction == null) {
         userTransaction = (UserTransaction) new GeronimoUserTransaction(getTransactionManager());
      }
      return userTransaction;
   }

   @Override
   public String toString() {
      return "GeronimoTransactionManagerLookup";
   }
}
