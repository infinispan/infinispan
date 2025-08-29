package org.infinispan.commons.query;

import org.infinispan.commons.api.query.HitCount;

public record TotalHitCount(int value, boolean exact) implements HitCount {

   public static final TotalHitCount EMPTY = new TotalHitCount(0, true);

}
