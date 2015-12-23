package org.infinispan.distribution.group;

import org.infinispan.configuration.cache.GroupsConfiguration;
import org.infinispan.factories.AbstractNamedCacheComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * @private
 */
@Scope(Scopes.NAMED_CACHE)
@DefaultFactoryFor(classes = GroupManager.class)
public class GroupManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   public <T> T construct(Class<T> componentType) {
      GroupsConfiguration groupsConfiguration = configuration.clustering().hash().groups();
      if (!groupsConfiguration.enabled())
         return null;

      return componentType.cast(new GroupManagerImpl(groupsConfiguration.groupers()));
   }
}
