package org.infinispan.test.hibernate.cache.commons.functional;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.naming.NamingException;
import javax.naming.Reference;

import org.hibernate.CustomEntityDirtinessStrategy;
import org.hibernate.HibernateException;
import org.hibernate.MappingException;
import org.hibernate.Session;
import org.hibernate.SessionFactoryObserver;
import org.hibernate.StatelessSession;
import org.hibernate.StatelessSessionBuilder;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.cache.spi.CacheImplementor;
import org.hibernate.context.spi.CurrentTenantIdentifierResolver;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.profile.FetchProfile;
import org.hibernate.engine.spi.FilterDefinition;
import org.hibernate.engine.spi.SessionBuilderImplementor;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SessionImplementor;
import org.hibernate.event.spi.EventEngine;
import org.hibernate.generator.Generator;
import org.hibernate.graph.spi.RootGraphImplementor;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.internal.FastSessionServices;
import org.hibernate.metamodel.model.domain.spi.JpaMetamodelImplementor;
import org.hibernate.metamodel.spi.MetamodelImplementor;
import org.hibernate.metamodel.spi.RuntimeMetamodelsImplementor;
import org.hibernate.proxy.EntityNotFoundDelegate;
import org.hibernate.query.BindableType;
import org.hibernate.query.criteria.HibernateCriteriaBuilder;
import org.hibernate.query.spi.QueryEngine;
import org.hibernate.relational.SchemaManager;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.type.Type;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.spi.TypeConfiguration;

import jakarta.persistence.EntityGraph;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceUnitUtil;
import jakarta.persistence.Query;
import jakarta.persistence.SynchronizationType;

public class NoOpSessionFactoryImplementor implements SessionFactoryImplementor {
   @Override
   public String getUuid() {
      return null;
   }

   @Override
   public String getName() {
      return null;
   }

   @Override
   public SessionImplementor openSession() {
      return null;
   }

   @Override
   public Session getCurrentSession() throws HibernateException {
      return null;
   }

   @Override
   public StatelessSessionBuilder withStatelessOptions() {
      return null;
   }

   @Override
   public StatelessSession openStatelessSession() {
      return null;
   }

   @Override
   public StatelessSession openStatelessSession(Connection connection) {
      return null;
   }

   @Override
   public TypeConfiguration getTypeConfiguration() {
      return null;
   }

   @Override
   public QueryEngine getQueryEngine() {
      return null;
   }

   @Override
   public EntityManager createEntityManager() {
      return null;
   }

   @Override
   public EntityManager createEntityManager(Map map) {
      return null;
   }

   @Override
   public EntityManager createEntityManager(SynchronizationType synchronizationType) {
      return null;
   }

   @Override
   public EntityManager createEntityManager(SynchronizationType synchronizationType, Map map) {
      return null;
   }

   @Override
   public HibernateCriteriaBuilder getCriteriaBuilder() {
      return null;
   }

   @Override
   public void close() throws HibernateException {

   }

   @Override
   public Map<String, Object> getProperties() {
      return null;
   }

   @Override
   public boolean isClosed() {
      return false;
   }

   @Override
   public SessionBuilderImplementor withOptions() {
      return null;
   }

   @Override
   public SessionImplementor openTemporarySession() throws HibernateException {
      return null;
   }

   @Override
   public CacheImplementor getCache() {
      return null;
   }

   @Override
   public PersistenceUnitUtil getPersistenceUnitUtil() {
      return null;
   }

   @Override
   public void addNamedQuery(String name, Query query) {

   }

   @Override
   public <T> T unwrap(Class<T> cls) {
      return null;
   }

   @Override
   public <T> void addNamedEntityGraph(String graphName, EntityGraph<T> entityGraph) {

   }

   @Override
   public <T> List<EntityGraph<? super T>> findEntityGraphsByType(Class<T> entityClass) {
      return null;
   }

   @Override
   public StatisticsImplementor getStatistics() {
      return null;
   }

   @Override
   public SchemaManager getSchemaManager() {
      return null;
   }

   @Override
   public RuntimeMetamodelsImplementor getRuntimeMetamodels() {
      return null;
   }

   @Override
   public JpaMetamodelImplementor getJpaMetamodel() {
      return null;
   }

   @Override
   public ServiceRegistryImplementor getServiceRegistry() {
      return null;
   }

   @Override
   public Integer getMaximumFetchDepth() {
      return null;
   }

   @Override
   public EventEngine getEventEngine() {
      return null;
   }

   @Override
   public FetchProfile getFetchProfile(String name) {
      return null;
   }

   @Override
   public Generator getGenerator(String rootEntityName) {
      return null;
   }

   @Override
   public EntityNotFoundDelegate getEntityNotFoundDelegate() {
      return null;
   }

   @Override
   public void addObserver(SessionFactoryObserver observer) {

   }

   @Override
   public CustomEntityDirtinessStrategy getCustomEntityDirtinessStrategy() {
      return null;
   }

   @Override
   public CurrentTenantIdentifierResolver<Object> getCurrentTenantIdentifierResolver() {
      return null;
   }

   @Override
   public JavaType<Object> getTenantIdentifierJavaType() {
      return null;
   }

   @Override
   public FastSessionServices getFastSessionServices() {
      return null;
   }

   @Override
   public WrapperOptions getWrapperOptions() {
      return null;
   }

   @Override
   public SessionFactoryOptions getSessionFactoryOptions() {
      return null;
   }

   @Override
   public FilterDefinition getFilterDefinition(String filterName) {
      return null;
   }

   @Override
   public Set<String> getDefinedFetchProfileNames() {
      return null;
   }

   @Override
   public JdbcServices getJdbcServices() {
      return null;
   }

   @Override
   public SqlStringGenerationContext getSqlStringGenerationContext() {
      return null;
   }

   @Override
   public RootGraphImplementor<?> findEntityGraphByName(String name) {
      return null;
   }

   @Override
   public Set<String> getDefinedFilterNames() {
      return null;
   }

   @Override
   public String bestGuessEntityName(Object object) {
      return null;
   }

   @Override
   public IdentifierGenerator getIdentifierGenerator(String rootEntityName) {
      return null;
   }

   @Override
   public DeserializationResolver<?> getDeserializationResolver() {
      return null;
   }

   @Override
   public MetamodelImplementor getMetamodel() {
      return null;
   }

   @Override
   public boolean isOpen() {
      return false;
   }

   @Override
   public <T> BindableType<? super T> resolveParameterBindType(T bindValue) {
      return null;
   }

   @Override
   public <T> BindableType<T> resolveParameterBindType(Class<T> clazz) {
      return null;
   }

   @Override
   public Reference getReference() throws NamingException {
      return null;
   }

   @Override
   public Type getIdentifierType(String className) throws MappingException {
      return null;
   }

   @Override
   public String getIdentifierPropertyName(String className) throws MappingException {
      return null;
   }

   @Override
   public Type getReferencedPropertyType(String className, String propertyName) throws MappingException {
      return null;
   }
}
