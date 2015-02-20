package org.infinispan.test.integration.as.jms.controller;

import org.hibernate.search.backend.impl.jms.AbstractJMSHibernateSearchController;
import org.hibernate.search.jpa.FullTextEntityManager;
import org.hibernate.search.jpa.Search;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.test.integration.as.jms.util.RegistrationConfiguration;

import javax.ejb.ActivationConfigProperty;
import javax.ejb.MessageDriven;
import javax.jms.MessageListener;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

@MessageDriven(activationConfig = {
      @ActivationConfigProperty(propertyName = "destinationType", propertyValue = "javax.jms.Queue"),
      @ActivationConfigProperty(propertyName = "destination", propertyValue = RegistrationConfiguration.DESTINATION_QUEUE)})
public class RegistrationMdb extends AbstractJMSHibernateSearchController implements MessageListener {

   @PersistenceContext
   private EntityManager em;

   @Override
   protected SearchIntegrator getSearchIntegrator() {
      FullTextEntityManager fullTextEntityManager = Search.getFullTextEntityManager(em);
      return fullTextEntityManager.getSearchFactory().unwrap(SearchIntegrator.class);
   }

}
