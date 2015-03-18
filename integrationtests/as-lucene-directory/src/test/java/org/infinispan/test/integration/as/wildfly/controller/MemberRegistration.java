package org.infinispan.test.integration.as.wildfly.controller;

import java.util.List;

import javax.annotation.PostConstruct;
import javax.ejb.Stateful;
import javax.enterprise.inject.Model;
import javax.enterprise.inject.Produces;
import javax.inject.Named;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.hibernate.search.MassIndexer;
import org.hibernate.search.jpa.Search;
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
