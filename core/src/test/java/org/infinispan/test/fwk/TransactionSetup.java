package org.infinispan.test.fwk;

import org.infinispan.commons.util.LegacyKeySupportSystemProperties;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.tx.lookup.GeronimoTransactionManagerLookup;

import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

/**
 * A simple abstraction for transaction manager interaction
 *
 * @author Jason T. Greene
 */
public class TransactionSetup {

   static {
      // make the log in-memory to make tests run faster. Note that the config is frozen at system initialization time,
      // so you need to set this before classloading the transaction system and can't change it within the same vm.
      JBossTransactionsUtils.setVolatileStores();
   }

   private interface Operations {
      UserTransaction getUserTransaction();

      String getLookup();

      TransactionManagerLookup lookup();

      void cleanup();

      TransactionManager getManager();
   }

   public static final String JBOSS_TM = "jbosstm";
   public static final String DUMMY_TM = "dummytm";
   public static final String GERONIMO_TM = "geronimotm";
   public static final String JTA = LegacyKeySupportSystemProperties.getProperty("infinispan.test.jta.tm", "infinispan.tm");

   private static Operations operations;

   static {
      init();
      }

   private static void init() {
      String property = JTA;
       if (DUMMY_TM.equalsIgnoreCase(property)) {
         System.out.println("Transaction manager used: Dummy");
         final String lookup = DummyTransactionManagerLookup.class.getName();
         final DummyTransactionManagerLookup instance = new DummyTransactionManagerLookup();
         operations = new Operations() {
            @Override
            public UserTransaction getUserTransaction() {
               return instance.getUserTransaction();
            }

            @Override
            public void cleanup() {
               instance.cleanup();
            }

            @Override
            public String getLookup() {
               return lookup;
            }

            @Override
            public TransactionManagerLookup lookup() {
               return instance;
            }

            @Override
            public TransactionManager getManager() {
               try {
                  return instance.getTransactionManager();
               }
               catch (Exception e) {
                  throw new RuntimeException(e);
               }

            }
         };
      } else if (GERONIMO_TM.equalsIgnoreCase(property)){
         System.out.println("Transaction manager used: Geronimo");
         final String lookup = GeronimoTransactionManagerLookup.class.getName();
         final GeronimoTransactionManagerLookup instance = new GeronimoTransactionManagerLookup();
         operations = new Operations() {
            @Override
            public UserTransaction getUserTransaction() {
               try {
                  return instance.getUserTransaction();
               }
               catch (Exception e) {
                  throw new RuntimeException(e);
               }
            }

            @Override
            public void cleanup() {
            }

            @Override
            public String getLookup() {
               return lookup;
            }

            @Override
            public TransactionManagerLookup lookup() {
               return instance;
            }

            @Override
            public TransactionManager getManager() {
               try {
                  return instance.getTransactionManager();
               }
               catch (Exception e) {
                  throw new RuntimeException(e);
               }
            }
         };
      } else {
          System.out.println("Transaction manager used: JBossTM");

          final String lookup = JBossStandaloneJTAManagerLookup.class.getName();
          final JBossStandaloneJTAManagerLookup instance = new JBossStandaloneJTAManagerLookup();
          operations = new Operations() {
             @Override
             public UserTransaction getUserTransaction() {
                try {
                   return instance.getUserTransaction();
                }
                catch (Exception e) {
                   throw new RuntimeException(e);
                }
             }

             @Override
             public void cleanup() {
             }

             @Override
             public String getLookup() {
                return lookup;
             }


             @Override
             public TransactionManagerLookup lookup() {
                return instance;
             }

             @Override
             public TransactionManager getManager() {
                try {
                   return instance.getTransactionManager();
                }
                catch (Exception e) {
                   throw new RuntimeException(e);
                }
             }

          };
       }
   }

   public static TransactionManager getManager() {
      return operations.getManager();
   }

   public static String getManagerLookup() {
      return operations.getLookup();
   }

   public static TransactionManagerLookup lookup() {
      return operations.lookup();
   }

   public static UserTransaction getUserTransaction() {
      return operations.getUserTransaction();
   }

   public static void cleanup() {
      operations.cleanup();
   }
}
