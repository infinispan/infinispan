package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.xsite.spi.XSiteEntryMergePolicy;

/**
 * A factory for {@link XSiteEntryMergePolicy}.
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
@DefaultFactoryFor(classes = XSiteEntryMergePolicy.class)
public class XSiteEntryMergePolicyFactory extends AbstractNamedCacheComponentFactory implements
      AutoInstantiableFactory {

   @Override
   public Object construct(String name) {
      assert name.equals(XSiteEntryMergePolicy.class.getName());
      return configuration.sites().mergePolicy();
   }
}
