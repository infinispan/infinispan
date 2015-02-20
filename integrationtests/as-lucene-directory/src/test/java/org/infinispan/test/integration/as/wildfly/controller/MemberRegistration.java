package org.infinispan.test.integration.as.wildfly.controller;

import org.apache.lucene.search.Query;
import org.hibernate.search.jpa.FullTextEntityManager;
import org.infinispan.test.integration.as.wildfly.model.Member;

import javax.annotation.PostConstruct;
import javax.ejb.Stateful;
import javax.enterprise.inject.Model;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;

@Stateful
@Model
public class MemberRegistration {

   @Inject
   private FullTextEntityManager em;

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
      Query luceneQuery = em.getSearchFactory().buildQueryBuilder()
            .forEntity(Member.class).get().keyword()
            .onField("name").matching(name)
            .createQuery();

      return em.createFullTextQuery(luceneQuery).getResultList();
   }

   @PostConstruct
   public void initNewMember() {
      newMember = new Member();
   }

}
