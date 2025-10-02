package org.infinispan.marshall.protostream.impl.adapters;

import static org.testng.AssertJUnit.assertEquals;


import java.security.Principal;
import java.util.Set;
import java.util.stream.Collectors;

import javax.security.auth.Subject;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "adapters.SubjectAdapterTest")
public class SubjectAdapterTest extends AbstractAdapterTest {

   public void testSubjectIsMarshallable() throws Exception {
      Subject original = new Subject();
      original.getPrincipals().add(new TestingPrincipal("p1"));
      original.getPrincipals().add(new TestingPrincipal("p2"));

      Subject deserialized = deserialize(original);

      Set<String> originalNames = original.getPrincipals().stream().map(Principal::getName).collect(Collectors.toSet());
      Set<String> deserializedNames = deserialized.getPrincipals().stream().map(Principal::getName).collect(Collectors.toSet());

      assertEquals(originalNames, deserializedNames);
   }

   public static class TestingPrincipal implements Principal {
      final String name;

      public TestingPrincipal(String name) {
         this.name = name;
      }

      @Override
      public String getName() {
         return name;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         TestingPrincipal that = (TestingPrincipal) o;

         return name.equals(that.name);
      }

      @Override
      public int hashCode() {
         return name.hashCode();
      }
   }
}
