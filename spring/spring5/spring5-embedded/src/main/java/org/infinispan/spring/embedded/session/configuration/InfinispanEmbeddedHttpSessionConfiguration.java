package org.infinispan.spring.embedded.session.configuration;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import org.infinispan.spring.embedded.provider.SpringEmbeddedCacheManager;
import org.infinispan.spring.embedded.session.InfinispanEmbeddedSessionRepository;
import org.infinispan.spring.common.provider.SpringCache;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportAware;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.session.MapSession;
import org.springframework.session.config.annotation.web.http.SpringHttpSessionConfiguration;


@Configuration
public class InfinispanEmbeddedHttpSessionConfiguration extends SpringHttpSessionConfiguration implements ImportAware {

   private String cacheName;
   private int maxInactiveIntervalInSeconds;

   @Bean
   public InfinispanEmbeddedSessionRepository sessionRepository(SpringEmbeddedCacheManager cacheManager, ApplicationEventPublisher eventPublisher) {
      Objects.requireNonNull(cacheName, "Cache name can not be null");
      Objects.requireNonNull(cacheManager, "Cache Manager can not be null");
      Objects.requireNonNull(eventPublisher, "Event Publisher can not be null");

      SpringCache cacheForSessions = cacheManager.getCache(cacheName);

      InfinispanEmbeddedSessionRepository sessionRepository = new InfinispanEmbeddedSessionRepository(cacheForSessions) {
         @Override
         public MapSession createSession() {
            MapSession session = super.createSession();
            session.setMaxInactiveInterval(Duration.ofSeconds(maxInactiveIntervalInSeconds));
            return session;
         }
      };
      sessionRepository.setApplicationEventPublisher(eventPublisher);

      return sessionRepository;
   }

   @Override
   public void setImportMetadata(AnnotationMetadata importMetadata) {
      Map<String, Object> enableAttrMap = importMetadata
            .getAnnotationAttributes(EnableInfinispanEmbeddedHttpSession.class.getName());
      AnnotationAttributes annotationAttributes = AnnotationAttributes.fromMap(enableAttrMap);
      cacheName = annotationAttributes.getString("cacheName");
      maxInactiveIntervalInSeconds = annotationAttributes.getNumber("maxInactiveIntervalInSeconds").intValue();
   }
}
