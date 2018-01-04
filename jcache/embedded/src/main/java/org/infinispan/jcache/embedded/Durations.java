package org.infinispan.jcache.embedded;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

import org.infinispan.functional.EntryView;
import org.infinispan.functional.MetaParam;
import org.infinispan.jcache.Expiration;

public class Durations {
   public static final MetaParam.MetaLifespan ETERNAL = new MetaParam.MetaLifespan(-1);

   public static long toMs(Duration duration) {
      if (duration.isEternal()) {
         return -1;
      }
      return duration.getTimeUnit().toMillis(duration.getDurationAmount());
   }

   public static <K, V> void setWithTtl(EntryView.ReadWriteEntryView<K, V> view, V value, ExpiryPolicy expiryPolicy, Expiration.Operation operation) {
      Duration ttl = Expiration.getExpiry(expiryPolicy, operation);
      if (ttl == null) {
         view.set(value);
      } else if (ttl.isEternal()) {
         view.set(value, ETERNAL);
      } else if (ttl.isZero()) {
         view.remove();
      } else {
         view.set(value, new MetaParam.MetaLifespan(toMs(ttl)));
      }
   }

   public static <K, V> void updateTtl(EntryView.ReadWriteEntryView<K, V> view, ExpiryPolicy expiryPolicy) {
      updateTtl(view, expiryPolicy, Expiration.Operation.ACCESS);
   }

   public static <K, V> void updateTtl(EntryView.ReadWriteEntryView<K, V> view, ExpiryPolicy expiryPolicy, Expiration.Operation operation) {
      if (!view.find().isPresent()) {
         return;
      }
      Duration ttl = Expiration.getExpiry(expiryPolicy, operation);
      if (ttl == null) {
         // noop
      } else if (ttl.isEternal()) {
         // do not update on read when already eternal
         if (view.findMetaParam(MetaParam.MetaLifespan.class).filter(lifespan -> lifespan.get() >= 0).isPresent()) {
            view.set(view.get(), ETERNAL);
         }
      } else if (ttl.isZero()) {
         view.remove();
      } else {
         view.set(view.get(), new MetaParam.MetaLifespan(toMs(ttl)));
      }
   }
}
