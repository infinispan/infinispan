package org.infinispan.distribution.groups;

import java.util.Collections;

import org.infinispan.distribution.DistAsyncFuncTest;
import org.infinispan.distribution.group.Grouper;
import org.testng.annotations.Test;

/**
 * @author Pete Muir
 * @since 5.0
 */
@Test (groups = "functional", testName = "distribution.groups.GroupsDistAsyncFuncTest")
public class GroupsDistAsyncFuncTest extends DistAsyncFuncTest {

   public GroupsDistAsyncFuncTest() {
      groupsEnabled = true;
      groupers = Collections.<Grouper<?>>singletonList(new KXGrouper());
   }
   
}
