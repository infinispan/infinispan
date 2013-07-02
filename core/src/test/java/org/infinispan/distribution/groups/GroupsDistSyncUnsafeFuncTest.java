package org.infinispan.distribution.groups;

import java.util.Collections;

import org.infinispan.distribution.DistSyncUnsafeFuncTest;
import org.infinispan.distribution.group.Grouper;
import org.testng.annotations.Test;

/**
 * @author Pete Muir
 * @since 5.0
 */
@Test(testName="distribution.groups.GroupsDistSyncUnsafeFuncTest", groups = "functional")
public class GroupsDistSyncUnsafeFuncTest extends DistSyncUnsafeFuncTest {
   
   public GroupsDistSyncUnsafeFuncTest() {
      groupsEnabled = true;
      groupers = Collections.<Grouper<?>>singletonList(new KXGrouper());
   }

}
