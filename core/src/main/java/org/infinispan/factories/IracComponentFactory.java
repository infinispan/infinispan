package org.infinispan.factories;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.BackupFailurePolicy;
import org.infinispan.container.versioning.irac.DefaultIracTombstoneManager;
import org.infinispan.container.versioning.irac.IracTombstoneManager;
import org.infinispan.container.versioning.irac.NoOpIracTombstoneManager;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.xsite.XSiteBackup;
import org.infinispan.xsite.irac.DefaultIracManager;
import org.infinispan.xsite.irac.IracManager;
import org.infinispan.xsite.irac.IracXSiteBackup;
import org.infinispan.xsite.irac.NoOpIracManager;

import net.jcip.annotations.GuardedBy;

@DefaultFactoryFor(classes = {
        IracManager.class,
        IracTombstoneManager.class
})
public class IracComponentFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

    @Inject Transport transport;
    @GuardedBy("this")
    private List<IracXSiteBackup> asyncBackups;

    @Override
    public Object construct(String name) {
        if (name.equals(IracTombstoneManager.class.getName())) {
            List<IracXSiteBackup> backups = getAsyncBackups();
            return backups.isEmpty() ?
                    NoOpIracTombstoneManager.getInstance() :
                    new DefaultIracTombstoneManager(configuration, backups);

        } else if (name.equals(IracManager.class.getName())) {
            List<IracXSiteBackup> backups = getAsyncBackups();
            return  backups.isEmpty() ?
                    NoOpIracManager.INSTANCE :
                    new DefaultIracManager(configuration, backups);
        }
        throw CONTAINER.factoryCannotConstructComponent(name);
    }

    private synchronized List<IracXSiteBackup> getAsyncBackups() {
        if (asyncBackups != null) {
            return asyncBackups;
        }
        if (transport == null) {
            return asyncBackups = Collections.emptyList();
        }
        // -1 since incrementAndGet is used!
        AtomicInteger index = new AtomicInteger(-1);
        asyncBackups = configuration.sites().asyncBackupsStream()
                .filter(this::isRemoteSite) // filter local site
                .map(c -> create(c, index)) //convert to sync
                .collect(Collectors.toList());
        if (log.isTraceEnabled()) {
            String b = asyncBackups.stream().map(XSiteBackup::getSiteName).collect(Collectors.joining(", "));
            log.tracef("Async remote sites found: %s", b);
        }
        if (!asyncBackups.isEmpty()) {
            transport.checkCrossSiteAvailable();
        }
        return asyncBackups;
    }

    private boolean isRemoteSite(BackupConfiguration config) {
        return !config.site().equals(transport.localSiteName());
    }

    private IracXSiteBackup create(BackupConfiguration config, AtomicInteger index) {
        return new IracXSiteBackup(config.site(),
                true,
                config.replicationTimeout(),
                config.backupFailurePolicy() == BackupFailurePolicy.WARN,
                (short) index.incrementAndGet());
    }
}
