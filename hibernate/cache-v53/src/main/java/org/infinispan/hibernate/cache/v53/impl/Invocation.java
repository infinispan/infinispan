package org.infinispan.hibernate.cache.v53.impl;

import java.util.concurrent.CompletableFuture;

interface Invocation {
   CompletableFuture<?> invoke(boolean success);
}
