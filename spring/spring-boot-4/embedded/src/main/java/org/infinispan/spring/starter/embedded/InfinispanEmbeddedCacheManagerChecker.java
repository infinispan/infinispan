package org.infinispan.spring.starter.embedded;

import org.springframework.boot.autoconfigure.cache.CacheType;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class InfinispanEmbeddedCacheManagerChecker implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        String cacheType = context.getEnvironment().getProperty("spring.cache.type");

        return cacheType == null || CacheType.INFINISPAN.name().equalsIgnoreCase(cacheType);
    }
}
