package org.infinispan.test.fwk;

import com.arjuna.ats.arjuna.common.ObjectStoreEnvironmentBean;
import com.arjuna.ats.arjuna.common.arjPropertyManager;
import com.arjuna.ats.internal.arjuna.objectstore.VolatileStore;
import com.arjuna.common.internal.util.propertyservice.BeanPopulator;

/**
 * Utility methods for JBoss Transactions (Arjuna) library.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public class JBossTransactionsUtils {

   /**
    * Sets volatile stores so that transaction logging happens in memory, both
    * making tests run faster and avoids environment issues as a result of
    * filesystem not being accessible.
    */
   public static void setVolatileStores() {
      String volatileStoreType = VolatileStore.class.getName();
      arjPropertyManager.getCoordinatorEnvironmentBean().setCommunicationStore(volatileStoreType);
      arjPropertyManager.getObjectStoreEnvironmentBean().setObjectStoreType(volatileStoreType);
      BeanPopulator.getNamedInstance(ObjectStoreEnvironmentBean.class,
            "communicationStore").setObjectStoreType(volatileStoreType);
   }

}
