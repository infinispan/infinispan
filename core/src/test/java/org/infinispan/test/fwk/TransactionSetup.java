package org.infinispan.test.fwk;

import jakarta.transaction.TransactionManager;
import jakarta.transaction.UserTransaction;

import org.infinispan.commons.util.LegacyKeySupportSystemProperties;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;

/**
 * A simple abstraction for transaction manager interaction
 *
 * @author Jason T. Greene
 */
public class TransactionSetup {

   private interface Operations {
      UserTransaction getUserTransaction();

      String getLookup();

      TransactionManagerLookup lookup();

      void cleanup();

      TransactionManager getManager();
   }

   public static final String JBOSS_TM = "jbosstm";
   public static final String DUMMY_TM = "dummytm";
   public static final String JTA = LegacyKeySupportSystemProperties.getProperty("infinispan.test.jta.tm", "infinispan.tm");

   private static Operations operations;

   static {
      init();
      }

   private static void init() {
      String property = JTA;
       if (DUMMY_TM.equalsIgnoreCase(property)) {
         System.out.println("Transaction manager used: Dummy");
         final String lookup = EmbeddedTransactionManagerLookup.class.getName();
         final EmbeddedTransactionManagerLookup instance = new EmbeddedTransactionManagerLookup();
         operations = new Operations() {
            @Override
            public UserTransaction getUserTransaction() {
               return EmbeddedTransactionManagerLookup.getUserTransaction();
            }

            @Override
            public void cleanup() {
               EmbeddedTransactionManagerLookup.cleanup();
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
