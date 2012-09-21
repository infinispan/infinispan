/*
 * Copyright 2012 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

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
