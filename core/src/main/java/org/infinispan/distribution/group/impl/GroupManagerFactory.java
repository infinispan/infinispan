package org.infinispan.distribution.group.impl;

import org.infinispan.configuration.cache.GroupsConfiguration;
import org.infinispan.factories.AbstractNamedCacheComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

@Scope(Scopes.NAMED_CACHE)
@DefaultFactoryFor(classes = GroupManager.class)
public class GroupManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      GroupsConfiguration groupsConfiguration = configuration.clustering().hash().groups();
      if (!groupsConfiguration.enabled())
         return null;

      return new GroupManagerImpl(groupsConfiguration.groupers());
   }
}
