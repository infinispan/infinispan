package org.infinispan.spring.remote.session.configuration;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.spring.remote.provider.SpringRemoteCacheManager;
import org.infinispan.spring.remote.session.InfinispanRemoteSessionRepository;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportAware;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.core.task.TaskExecutor;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.session.MapSession;
import org.springframework.session.config.annotation.web.http.SpringHttpSessionConfiguration;

@Configuration
public class InfinispanRemoteHttpSessionConfiguration extends SpringHttpSessionConfiguration implements ImportAware {

   private String cacheName;
   private int maxInactiveIntervalInSeconds;
   private int executorPoolSize;
   private int executorMaxPoolSize;
   private String executorThreadNamePrefix;

   @Bean
   public TaskExecutor infinispanRemoteTaskExecutor() {
      ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
      executor.setCorePoolSize(executorPoolSize);
      executor.setMaxPoolSize(executorMaxPoolSize);
      executor.setThreadNamePrefix(executorThreadNamePrefix);
      executor.initialize();
      return executor;
   }

   @Bean
   public InfinispanRemoteSessionRepository sessionRepository(SpringRemoteCacheManager cacheManager,
                                                              ApplicationEventPublisher eventPublisher,
                                                              @Qualifier("infinispanRemoteTaskExecutor") TaskExecutor taskExecutor) {
      Objects.requireNonNull(cacheName, "Cache name can not be null");
      Objects.requireNonNull(cacheManager, "Cache Manager can not be null");
      Objects.requireNonNull(eventPublisher, "Event Publisher can not be null");

      SpringCache cacheForSessions = cacheManager.getCache(cacheName);

      InfinispanRemoteSessionRepository sessionRepository = new InfinispanRemoteSessionRepository(cacheForSessions, taskExecutor) {
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
            .getAnnotationAttributes(EnableInfinispanRemoteHttpSession.class.getName());
      AnnotationAttributes annotationAttributes = AnnotationAttributes.fromMap(enableAttrMap);
      cacheName = annotationAttributes.getString("cacheName");
      maxInactiveIntervalInSeconds = annotationAttributes.getNumber("maxInactiveIntervalInSeconds").intValue();
      executorThreadNamePrefix = annotationAttributes.getString("executorThreadNamePrefix");
      executorPoolSize = annotationAttributes.getNumber("executorPoolSize").intValue();
      executorMaxPoolSize = annotationAttributes.getNumber("executorMaxPoolSize").intValue();
   }
}
