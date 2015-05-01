package org.infinispan.test.integration.as.wildfly.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.ejb.Stateful;
import javax.enterprise.inject.Model;
import javax.enterprise.inject.Produces;
import javax.inject.Named;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.hibernate.search.MassIndexer;
import org.hibernate.search.SearchFactory;
import org.hibernate.search.annotations.Spatial;
import org.hibernate.search.jpa.FullTextQuery;
import org.hibernate.search.jpa.Search;
import org.hibernate.search.query.dsl.Unit;
import org.infinispan.test.integration.as.wildfly.model.Member;

@Stateful
@Model
public class MemberRegistration {

   @PersistenceContext
   private EntityManager em;

   private Member newMember;

   @Produces
   @Named
   public Member getNewMember() {
      return newMember;
   }

   public void register() throws Exception {
      register(newMember);
   }

   public void register(Member member) throws Exception {
      em.persist(member);
      initNewMember();
   }

   @SuppressWarnings("unchecked")
   public List<Member> search(String name) {
      Query luceneQuery = Search.getFullTextEntityManager(em).getSearchFactory().buildQueryBuilder()
            .forEntity(Member.class).get().keyword().onField("name").matching(name).createQuery();

      return Search.getFullTextEntityManager(em).createFullTextQuery(luceneQuery).getResultList();
   }

   @SuppressWarnings("unchecked")
   public List<Member> spatialSearch(double latitude, double longitude, double distanceinKM) {
      Query spatialQuery = Search.getFullTextEntityManager(em).getSearchFactory()
            .buildQueryBuilder().forEntity( Member.class ).get().spatial()
            .within( distanceinKM, Unit.KM )
            .ofLatitude( latitude )
            .andLongitude( longitude )
            .createQuery();
      return Search.getFullTextEntityManager(em).createFullTextQuery(spatialQuery, Member.class).getResultList();
   }

   @SuppressWarnings("unchecked")
   public List<Object[]> spatialSearchWithDistance(double latitude, double longitude, double distanceinKM) {
      Query spatialQuery = Search.getFullTextEntityManager(em).getSearchFactory()
            .buildQueryBuilder().forEntity(Member.class).get().spatial()
            .within(distanceinKM, Unit.KM)
            .ofLatitude(latitude)
            .andLongitude(longitude)
            .createQuery();

      FullTextQuery hibQuery = Search.getFullTextEntityManager(em).createFullTextQuery(spatialQuery, Member.class);
      hibQuery.setProjection(FullTextQuery.SPATIAL_DISTANCE, FullTextQuery.THIS);
      hibQuery.setSpatialParameters(latitude, longitude, Spatial.COORDINATES_DEFAULT_FIELD);
      return hibQuery.getResultList();
   }

   public List<Document> indexSearch(String name) throws IOException {
      ArrayList<Document> result = new ArrayList<Document>();
      SearchFactory searchFactory = Search.getFullTextEntityManager(em).getSearchFactory();
      IndexReader reader = searchFactory.getIndexReaderAccessor().open(Member.class);
      try {
         for (int i = 0; i < reader.maxDoc(); i++) {
            Document member = reader.document(i);
            if (member != null && member.get("name").contains(name)) {
               result.add(member);
            }
         }
      } finally {
         searchFactory.getIndexReaderAccessor().close(reader);
      }
      return result;
   }

   public int indexSize() {
      SearchFactory searchFactory = Search.getFullTextEntityManager(em).getSearchFactory();
      IndexReader reader = searchFactory.getIndexReaderAccessor().open(Member.class);
      try {
         return reader.maxDoc();
      } finally {
         searchFactory.getIndexReaderAccessor().close(reader);
      }
   }

   @SuppressWarnings("unchecked")
   public List<Member> luceneSearch(String name) throws ParseException {
      QueryParser parser = new QueryParser("name", new StandardAnalyzer());
      org.apache.lucene.search.Query luceneQuery = parser.parse(name);

      return Search.getFullTextEntityManager(em).createFullTextQuery(luceneQuery).getResultList();
   }

   public void purgeMemberIndex() {
      Search.getFullTextEntityManager(em).purgeAll(Member.class);
   }

   public void indexMembers() throws InterruptedException {
      MassIndexer mi = Search.getFullTextEntityManager(em).createIndexer(Member.class);
      mi.startAndWait();
   }

   @PostConstruct
   public void initNewMember() {
      newMember = new Member();
   }

}
