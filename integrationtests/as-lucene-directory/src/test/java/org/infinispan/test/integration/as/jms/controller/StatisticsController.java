package org.infinispan.test.integration.as.jms.controller;

import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;

import javax.ejb.Stateless;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;

/**
 * Accessor to the {@link org.hibernate.SessionFactory} statistics.
 *
 * @author Davide D'Alto
 */
@Stateless
public class StatisticsController {

   @PersistenceUnit
   private EntityManagerFactory factory;

   public Statistics getStatistics() {
      return factory.unwrap(SessionFactory.class).getStatistics();
   }
}
