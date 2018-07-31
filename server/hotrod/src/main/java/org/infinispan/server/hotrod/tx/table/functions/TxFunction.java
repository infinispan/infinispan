package org.infinispan.server.hotrod.tx.table.functions;

import java.util.function.Function;

import org.infinispan.commands.functional.functions.InjectableComponent;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.server.hotrod.tx.table.CacheXid;
import org.infinispan.server.hotrod.tx.table.TxState;
import org.infinispan.util.TimeService;

/**
 * //TODO document this!
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public abstract class TxFunction implements Function<EntryView.ReadWriteEntryView<CacheXid, TxState>, Byte>,
      InjectableComponent {

   protected TimeService timeService;

   @Override
   public void inject(ComponentRegistry registry) {
      timeService = registry.getTimeService();
   }
}
