package org.infinispan.client.hotrod.impl.operations;

import java.time.Duration;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.Flags;

public class AdvancedHotRodOperation<OP> extends DelegatingHotRodOperation<OP> {
   private final CacheOptions options;
   private final int privateFlags;

   public AdvancedHotRodOperation(HotRodOperation<OP> delegate, CacheOptions options, int privateFlags) {
      super(delegate);
      this.options = options;
      this.privateFlags = privateFlags;
   }

   public AdvancedHotRodOperation(HotRodOperation<OP> delegate, CacheOptions options) {
      this(delegate, options, 0);
   }

   @Override
   public int flags() {
      return options.flags()
            .map(Flags::toInt)
            .orElseGet(super::flags) | privateFlags;
   }

   @Override
   public long timeout() {
      return options.timeout()
            .map(Duration::toMillis)
            .orElseGet(super::timeout);
   }
}
