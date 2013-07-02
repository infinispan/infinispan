package org.infinispan.tx;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "tx.TxCompletionForRolledBackXaTxTest")
public class TxCompletionForRolledBackXaTxTest extends TxCompletionForRolledBackTxTest {

   @Override
   protected void amend(ConfigurationBuilder dcc) {
      dcc.transaction().useSynchronization(false);
   }
}
