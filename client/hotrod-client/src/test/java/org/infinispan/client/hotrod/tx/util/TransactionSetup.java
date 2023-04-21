package org.infinispan.client.hotrod.tx.util;

import org.infinispan.client.hotrod.configuration.RemoteCacheConfigurationBuilder;
import org.infinispan.client.hotrod.transaction.lookup.RemoteTransactionManagerLookup;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.commons.util.LegacyKeySupportSystemProperties;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;

import jakarta.transaction.TransactionManager;

/**
 * Setup the {@link TransactionManager} for test classes.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public final class TransactionSetup {

   private static final String JBOSS_TM = "jbosstm";
   private static final String DUMMY_TM = "dummytm";
   private static final String JTA = LegacyKeySupportSystemProperties
         .getProperty("infinispan.test.jta.tm.hotrod", "infinispan.tm.hotrod");
   private static TransactionManagerLookup lookup;

   static {
      init();
   }

   private TransactionSetup() {
   }

   private static void init() {
      String property = JTA;
      if (DUMMY_TM.equalsIgnoreCase(property)) {
         System.out.println("Hot Rod client transaction manager used: Dummy");
         lookup = RemoteTransactionManagerLookup.getInstance();
      } else {
         //use JBossTM as default (as in core)
         System.out.println("Hot Rod client transaction manager used: JBossTM");
         JBossStandaloneJTAManagerLookup tmLookup = new JBossStandaloneJTAManagerLookup();
         tmLookup.init(new GlobalConfigurationBuilder().build());
         lookup = tmLookup;
      }
   }

   public static RemoteCacheConfigurationBuilder amendJTA(RemoteCacheConfigurationBuilder builder) {
      return builder.transactionManagerLookup(lookup);
   }

}
