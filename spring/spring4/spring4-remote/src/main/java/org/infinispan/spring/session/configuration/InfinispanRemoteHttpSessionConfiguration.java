package org.infinispan.spring.session.configuration;

import java.util.Map;
import java.util.Objects;

import org.infinispan.spring.provider.SpringCache;
import org.infinispan.spring.provider.SpringRemoteCacheManager;
import org.infinispan.spring.session.InfinispanRemoteSessionRepository;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportAware;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.session.MapSession;
import org.springframework.session.config.annotation.web.http.SpringHttpSessionConfiguration;

@Configuration
public class InfinispanRemoteHttpSessionConfiguration extends SpringHttpSessionConfiguration implements ImportAware {

   private String cacheName;
   private int maxInactiveIntervalInSeconds;

   @Bean
   public InfinispanRemoteSessionRepository sessionRepository(SpringRemoteCacheManager cacheManager, ApplicationEventPublisher eventPublisher) {
      Objects.requireNonNull(cacheName, "Cache name can not be null");
      Objects.requireNonNull(cacheManager, "Cache Manager can not be null");
      Objects.requireNonNull(eventPublisher, "Event Publisher can not be null");

      SpringCache cacheForSessions = cacheManager.getCache(cacheName);

      InfinispanRemoteSessionRepository sessionRepository = new InfinispanRemoteSessionRepository(cacheForSessions) {
         @Override
         public MapSession createSession() {
            MapSession session = super.createSession();
            session.setMaxInactiveIntervalInSeconds(maxInactiveIntervalInSeconds);
            return session;
         }
      };
      sessionRepository.setApplicationEventPublisher(eventPublisher);

      return sessionRepository;
   }

   @Override
   public void setImportMetadata(AnnotationMetadata importMetadata) {
      Map<String, Object> enableAttrMap = importMetadata
            .getAnnotationAttributes(EnableInfinispanRemoteHttpSession.class.getName());
      AnnotationAttributes annotationAttributes = AnnotationAttributes.fromMap(enableAttrMap);
      cacheName = annotationAttributes.getString("cacheName");
      maxInactiveIntervalInSeconds = annotationAttributes.getNumber("maxInactiveIntervalInSeconds").intValue();
   }
}
