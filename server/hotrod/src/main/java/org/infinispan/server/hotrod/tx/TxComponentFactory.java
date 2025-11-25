package org.infinispan.server.hotrod.tx;

import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.server.hotrod.tx.table.GlobalTxTable;
import org.infinispan.util.logging.Log;

@DefaultFactoryFor(classes = {
      GlobalTxTable.class,
})
@Scope(Scopes.GLOBAL)
public class TxComponentFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      if (componentName.equals(GlobalTxTable.class.getName())) {
         return new GlobalTxTable();
      }

      throw Log.CONTAINER.factoryCannotConstructComponent(componentName);
   }
}
