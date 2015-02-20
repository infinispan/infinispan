package org.infinispan.test.integration.as.jms.controller;

import org.apache.lucene.search.Query;
import org.hibernate.search.jpa.FullTextEntityManager;
import org.hibernate.search.jpa.Search;
import org.infinispan.test.integration.as.jms.model.RegisteredMember;

import javax.annotation.PostConstruct;
import javax.ejb.Stateful;
import javax.inject.Named;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.List;

@Stateful
public class RegistrationController {

   @PersistenceContext
   private EntityManager em;

   private RegisteredMember newMember;

   @Named
   public RegisteredMember getNewMember() {
      return newMember;
   }

   public void register() throws Exception {
      em.persist(newMember);
      resetNewMember();
   }

   public int deleteAllMembers() throws Exception {
      return em.createQuery("DELETE FROM RegisteredMember").executeUpdate();
   }

   public RegisteredMember findById(Long id) {
      return em.find(RegisteredMember.class, id);
   }

   @SuppressWarnings("unchecked")
   public List<RegisteredMember> search(String name) {
      FullTextEntityManager fullTextEm = Search.getFullTextEntityManager(em);
      Query luceneQuery = fullTextEm.getSearchFactory().buildQueryBuilder()
            .forEntity(RegisteredMember.class).get()
            .keyword().onField("name").matching(name).createQuery();

      return fullTextEm.createFullTextQuery(luceneQuery).getResultList();
   }

   @PostConstruct
   public void resetNewMember() {
      newMember = new RegisteredMember();
   }

}
