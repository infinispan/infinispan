package org.infinispan.hibernate.cache.v6.impl;

import java.util.concurrent.CompletableFuture;

interface Invocation {
   CompletableFuture<?> invoke(boolean success);
}
