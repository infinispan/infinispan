package org.infinispan.jcache.embedded.functions;

import java.util.function.Function;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.functional.EntryView;
import org.infinispan.protostream.annotations.ProtoTypeId;

@ProtoTypeId(ProtoStreamTypeIds.JCACHE_REMOVE)
public class Remove<K, V> implements Function<EntryView.ReadWriteEntryView<K, V>, Boolean> {
   private static final Remove INSTANCE = new Remove();

   public static <K, V> Remove<K, V> getInstance() {
      return INSTANCE;
   }

   @Override
   public Boolean apply(EntryView.ReadWriteEntryView<K, V> view) {
      boolean exists = view.peek().isPresent();
      // Contrary to view.remove() this forces the remove to be persisted even if it does not exist
      view.set(null);
      return exists;
   }
}
