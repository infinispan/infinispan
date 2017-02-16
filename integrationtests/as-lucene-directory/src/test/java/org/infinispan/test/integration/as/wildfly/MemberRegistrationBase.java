package org.infinispan.test.integration.as.wildfly;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import javax.ejb.EJBTransactionRolledbackException;
import javax.inject.Inject;

import org.apache.lucene.document.Document;
import org.infinispan.test.integration.as.wildfly.controller.MemberRegistration;
import org.infinispan.test.integration.as.wildfly.model.Member;
import org.jboss.arquillian.container.test.api.OperateOnDeployment;
import org.jboss.arquillian.junit.InSequence;
import org.junit.Test;

@SuppressWarnings("unused")
class MemberRegistrationBase {

   private final static double GD_LATITUDE = 37.769645;
   private final static double GD_LONGITUDE = -122.446428;
   private final static double CH_LATITUDE = 37.780392;
   private final static double CH_LONGITUDE = -122.513898;
   private final static double CBGB_LATITUDE = 40.726157;
   private final static double CBGB_LONGITUDE = -73.992116;
   private final static double KD_LATITUDE = 40.723165;
   private final static double KD_LONGITUDE = -73.987439;
   private final static double BG_LATITUDE = 41.874808;
   private final static double BG_LONGITUDE = -87.625983;
   @Inject
   MemberRegistration memberRegistration;

   @Test
   @InSequence(value = 1)
   @OperateOnDeployment("dep.active-1")
   public void
   testRegister() throws Exception {
      Member newMember = memberRegistration.getNewMember();
      newMember.setName("Davide D'Alto");
      newMember.setEmail("davide@mailinator.com");
      newMember.setPhoneNumber("2125551234");
      newMember.setLatitude(CH_LATITUDE);
      newMember.setLongitude(CH_LONGITUDE);
      memberRegistration.register();

      assertNotNull(newMember.getId());
      assertEquals("Index size isn't correct", 1, memberRegistration.indexSize());
   }

   @Test(expected = EJBTransactionRolledbackException.class)
   @InSequence(value = 2)
   @OperateOnDeployment("dep.active-1")
   public void testRegisterConstraint() throws Exception {
      Member newMember = memberRegistration.getNewMember();
      newMember.setName("Davide D'Altoe");
      newMember.setEmail("davide@mailinator.com");
      newMember.setPhoneNumber("2125551235");
      newMember.setLatitude(CH_LATITUDE);
      newMember.setLongitude(CH_LONGITUDE);
      memberRegistration.register();
   }

   @Test
   @InSequence(value = 3)
   @OperateOnDeployment("dep.active-2")
   public void testNewMemberSearch() throws Exception {
      Member newMember = memberRegistration.getNewMember();
      newMember.setName("Peter O'Tall");
      newMember.setEmail("peter@mailinator.com");
      newMember.setPhoneNumber("4643646643");
      newMember.setLatitude(KD_LATITUDE);
      newMember.setLongitude(KD_LONGITUDE);
      memberRegistration.register();

      List<Member> search = memberRegistration.search("Peter");

      assertFalse("Expected at least one result after the indexing", search.isEmpty());
      assertEquals("Search hasn't found a new member", newMember.getName(), search.get(0).getName());
      assertEquals("Index size isn't correct", 2, memberRegistration.indexSize());
   }

   @Test
   @InSequence(value = 4)
   @OperateOnDeployment("dep.active-2")
   public void testNewMemberLuceneSearch() throws Exception {
      List<Member> search = memberRegistration.luceneSearch("Peter");

      assertFalse("Expected at least one result after the indexing", search.isEmpty());
      assertEquals("Lucene search hasn't found a member", "Peter O'Tall", search.get(0).getName());
   }

   @Test
   @InSequence(value = 5)
   @OperateOnDeployment("dep.active-2")
   public void testNewMemberIndexSearch() throws Exception {
      List<Document> search = memberRegistration.indexSearch("Peter");

      assertFalse("Expected at least one result after the indexing", search.isEmpty());
      assertEquals("Lucene search hasn't found a member", "Peter O'Tall", search.get(0).get("name"));
   }

   @Test
   @InSequence(value = 6)
   @OperateOnDeployment("dep.active-2")
   public void testNonExistingMember() throws Exception {
      List<Member> search = memberRegistration.search("TotallyInventedName");

      assertNotNull("Search should never return null", search);
      assertTrue("Search results should be empty", search.isEmpty());
   }

   @Test
   @InSequence(value = 7)
   @OperateOnDeployment("dep.active-2")
   public void testLuceneNonExistingMember() throws Exception {
      List<Member> search = memberRegistration.luceneSearch("TotallyInventedName");

      assertNotNull("Search should never return null", search);
      assertTrue("Search results should be empty", search.isEmpty());
   }

   @Test
   @InSequence(value = 8)
   @OperateOnDeployment("dep.active-2")
   public void testIndexNonExistingMember() throws Exception {
      List<Document> search = memberRegistration.indexSearch("TotallyInventedName");

      assertNotNull("Search should never return null", search);
      assertTrue("Search results should be empty", search.isEmpty());
   }

   @Test
   @InSequence(value = 9)
   @OperateOnDeployment("dep.active-2")
   public void testPurgeIndex() throws Exception {
      memberRegistration.purgeMemberIndex();
      List<Member> search = memberRegistration.search("Peter");

      assertNotNull("Search should never return null", search);
      assertTrue("Search results should be empty", search.isEmpty());
      assertEquals("Index size isn't correct", 0, memberRegistration.indexSize());
   }

   @Test
   @InSequence(value = 10)
   @OperateOnDeployment("dep.active-2")
   public void testReIndex() throws Exception {
      memberRegistration.indexMembers();
      List<Member> search = memberRegistration.search("Peter");

      assertFalse("Expected at least one result after the indexing", search.isEmpty());
      assertEquals("Search hasn't found a new member after reindex", "Peter O'Tall", search.get(0).getName());
      assertEquals("Index size isn't correct", 2, memberRegistration.indexSize());
   }

   @Test
   @InSequence(value = 11)
   @OperateOnDeployment("dep.active-1")
   public void testNewMemberSpatialSearchNearGD() throws Exception {
      List<Member> members = memberRegistration.spatialSearch(GD_LATITUDE, GD_LONGITUDE, 10);
      assertEquals("Expected one result from spatial search", 1, members.size());
      assertEquals("Spatial search did not find the correct member", "Davide D'Alto", members.get(0).getName());
   }

   @Test
   @InSequence(value = 12)
   @OperateOnDeployment("dep.active-1")
   public void testNewMemberSpatialSearchNearGDWithDistance() throws Exception {
      List<Object[]> membersWithDistance = memberRegistration.spatialSearchWithDistance(GD_LATITUDE, GD_LONGITUDE, 10);
      assertEquals("Expected one result from spatial search", 1, membersWithDistance.size());
      assertEquals("Spatial search did not find the correct member", "Davide D'Alto",
              ((Member) membersWithDistance.get(0)[1]).getName());
      assertTrue("Distance was not greater than zero", (Double) membersWithDistance.get(0)[0] > 0);
   }

   @Test
   @InSequence(value = 13)
   @OperateOnDeployment("dep.active-1")
   public void testNewMemberSpatialSearchNearCBGB() throws Exception {
      List<Member> members = memberRegistration.spatialSearch(CBGB_LATITUDE, CBGB_LONGITUDE, 10);
      assertEquals("Expected one result from spatial search", 1, members.size());
      assertEquals("Spatial search did not find the correct member", "Peter O'Tall", members.get(0).getName());
   }

   @Test
   @InSequence(value = 14)
   @OperateOnDeployment("dep.active-1")
   public void testNewMemberSpatialSearchNearCBGBWithDistance() throws Exception {
      List<Object[]> membersWithDistance = memberRegistration.spatialSearchWithDistance(CBGB_LATITUDE, CBGB_LONGITUDE,
              10);
      assertEquals("Expected one result from spatial search", 1, membersWithDistance.size());
      assertEquals("Spatial search did not find the correct member", "Peter O'Tall",
              ((Member) membersWithDistance.get(0)[1]).getName());
      assertTrue("Distance was not greater than zero", (Double) membersWithDistance.get(0)[0] > 0);
   }

   @Test
   @InSequence(value = 15)
   @OperateOnDeployment("dep.active-1")
   public void testNewMemberSpatialSearchNearBG() throws Exception {
      List<Member> members = memberRegistration.spatialSearch(BG_LATITUDE, BG_LONGITUDE, 10);
      assertEquals("Expected one result from spatial search", 0, members.size());
   }

   @Test
   @InSequence(value = 16)
   @OperateOnDeployment("dep.active-1")
   public void testNewMemberSpatialSearchNearBGWithDistance() throws Exception {
      List<Object[]> membersWithDistance = memberRegistration.spatialSearchWithDistance(BG_LATITUDE, BG_LONGITUDE, 10);
      assertEquals("Expected one result from spatial search", 0, membersWithDistance.size());
   }

   @Test
   @InSequence(value = 17)
   @OperateOnDeployment("dep.active-1")
   public void testNewMemberSpatialSearchLongDistance() throws Exception {
      List<Member> members = memberRegistration.spatialSearch(GD_LATITUDE, GD_LONGITUDE, 5000);
      assertEquals("Expected one result from spatial search", 2, members.size());
      assertEquals("Spatial search did not find the correct member", "Davide D'Alto", members.get(0).getName());
      assertEquals("Spatial search did not find the correct member", "Peter O'Tall", members.get(1).getName());
   }

   @Test
   @InSequence(value = 18)
   @OperateOnDeployment("dep.active-1")
   public void testNewMemberSpatialSearchLongDistanceWithDistance() throws Exception {
      List<Object[]> membersWithDistance = memberRegistration
              .spatialSearchWithDistance(GD_LATITUDE, GD_LONGITUDE, 5000);
      assertEquals("Expected one result from spatial search", 2, membersWithDistance.size());
      assertEquals("Spatial search did not find the correct member", "Davide D'Alto",
              ((Member) membersWithDistance.get(0)[1]).getName());
      assertTrue("Distance was not greater than zero", (Double) membersWithDistance.get(0)[0] > 0);
      assertEquals("Spatial search did not find the correct member", "Peter O'Tall",
              ((Member) membersWithDistance.get(1)[1]).getName());
      assertTrue("Distance was not greater than zero", (Double) membersWithDistance.get(1)[0] > 0);
   }
}
