package org.infinispan.client.hotrod.tx.util;

import javax.transaction.TransactionManager;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.transaction.lookup.RemoteTransactionManagerLookup;
import org.infinispan.commons.util.LegacyKeySupportSystemProperties;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.transaction.lookup.JBossStandaloneJTAManagerLookup;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.util.tx.lookup.GeronimoTransactionManagerLookup;

/**
 * Setup the {@link TransactionManager} for test classes.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public final class TransactionSetup {

   private static final String JBOSS_TM = "jbosstm";
   private static final String DUMMY_TM = "dummytm";
   private static final String GERONIMO_TM = "geronimotm";
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
      } else if (GERONIMO_TM.equalsIgnoreCase(property)) {
         System.out.println("Hot Rod client transaction manager used: Geronimo");
         GeronimoTransactionManagerLookup tmLookup = new GeronimoTransactionManagerLookup();
         tmLookup.init(new GlobalConfigurationBuilder().build());
         lookup = tmLookup;
      } else {
         //use JBossTM as default (as in core)
         System.out.println("Hot Rod client transaction manager used: JBossTM");
         JBossStandaloneJTAManagerLookup tmLookup = new JBossStandaloneJTAManagerLookup();
         tmLookup.init(new GlobalConfigurationBuilder().build());
         lookup = tmLookup;
      }
   }

   public static void amendJTA(ConfigurationBuilder builder) {
      builder.transaction().transactionManagerLookup(lookup);
   }

}
