package org.infinispan.security;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Collections;

import org.infinispan.security.impl.CommonNameRoleMapper;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(testName = "security.RoleMapperTest")
public class RoleMapperTest {

   public void testCommonNameRoleMapper() {
      CommonNameRoleMapper mapper = new CommonNameRoleMapper();
      assertEquals(Collections.singleton("MadHatter"), mapper.principalToRoles(new TestingUtil.TestPrincipal("CN=MadHatter,OU=Users,DC=infinispan,DC=org")));
      assertEquals(Collections.singleton("MadHatter"), mapper.principalToRoles(new TestingUtil.TestPrincipal("cn=MadHatter,ou=Users,dc=infinispan,dc=org")));
   }
}
