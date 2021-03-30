package org.infinispan.xsite.statetransfer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheTopology;

/**
 * Decorator for {@link XSiteStateTransferManager}.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
@Scope(Scopes.NAMED_CACHE)
public class AbstractDelegatingXSiteStateTransferManager implements XSiteStateTransferManager {

   private final XSiteStateTransferManager delegate;

   public AbstractDelegatingXSiteStateTransferManager(XSiteStateTransferManager delegate) {
      this.delegate = delegate;
   }

   public static <T extends AbstractDelegatingXSiteStateTransferManager> T wrapCache(Cache<?, ?> cache,
         Function<XSiteStateTransferManager, T> ctor, Class<T> tClass) {
      XSiteStateTransferManager actual = TestingUtil.extractComponent(cache, XSiteStateTransferManager.class);
      if (actual.getClass().isAssignableFrom(tClass)) {
         return tClass.cast(actual);
      }
      return TestingUtil.wrapComponent(cache, XSiteStateTransferManager.class, ctor);
   }

   public static void revertXsiteStateTransferManager(Cache<?, ?> cache) {
      XSiteStateTransferManager manager = TestingUtil.extractComponent(cache, XSiteStateTransferManager.class);
      if (manager instanceof AbstractDelegatingXSiteStateTransferManager) {
         TestingUtil.replaceComponent(cache, XSiteStateTransferManager.class,
               ((AbstractDelegatingXSiteStateTransferManager) manager).delegate, true);
      }
   }

   @Override
   public void notifyStatePushFinished(String siteName, Address node, boolean statusOk) {
      delegate.notifyStatePushFinished(siteName, node, statusOk);
   }

   @Override
   public void startPushState(String siteName) throws Throwable {
      delegate.startPushState(siteName);
   }

   @Override
   public void cancelPushState(String siteName) throws Throwable {
      delegate.cancelPushState(siteName);
   }

   @Override
   public List<String> getRunningStateTransfers() {
      return delegate.getRunningStateTransfers();
   }

   @Override
   public Map<String, StateTransferStatus> getStatus() {
      return delegate.getStatus();
   }

   @Override
   public void clearStatus() {
      delegate.clearStatus();
   }

   @Override
   public Map<String, StateTransferStatus> getClusterStatus() {
      return delegate.getClusterStatus();
   }

   @Override
   public void clearClusterStatus() {
      delegate.clearClusterStatus();
   }

   @Override
   public String getSendingSiteName() {
      return delegate.getSendingSiteName();
   }

   @Override
   public void cancelReceive(String siteName) throws Exception {
      delegate.cancelReceive(siteName);
   }

   @Override
   public void becomeCoordinator(String siteName) {
      delegate.becomeCoordinator(siteName);
   }

   @Override
   public void onTopologyUpdated(CacheTopology cacheTopology, boolean stateTransferInProgress) {
      delegate.onTopologyUpdated(cacheTopology, stateTransferInProgress);
   }

   @Override
   public XSiteStateProvider getStateProvider() {
      return delegate.getStateProvider();
   }

   @Override
   public XSiteStateConsumer getStateConsumer() {
      return delegate.getStateConsumer();
   }

   @Override
   public void startAutomaticStateTransfer(Collection<String> sites) {
      delegate.startAutomaticStateTransfer(sites);
   }

   @Override
   public XSiteStateTransferMode stateTransferMode(String site) {
      return delegate.stateTransferMode(site);
   }

   @Override
   public boolean setAutomaticStateTransfer(String site, XSiteStateTransferMode mode) {
      return delegate.setAutomaticStateTransfer(site, mode);
   }
}
