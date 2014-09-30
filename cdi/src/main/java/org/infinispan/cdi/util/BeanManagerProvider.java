package org.infinispan.cdi.util;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.AfterDeploymentValidation;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.BeforeShutdown;
import javax.enterprise.inject.spi.Extension;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;




/**
 * <p>This class provides access to the {@link BeanManager}
 * by registering the current {@link BeanManager} in an extension and
 * making it available via a singleton factory for the current application.</p>
 * <p>This is really handy if you like to access CDI functionality
 * from places where no injection is available.</p>
 * <p>If a simple but manual bean-lookup is needed, it's easier to use the {@link BeanProvider}.</p>
 * <p/>
 * <p>As soon as an application shuts down, the reference to the {@link BeanManager} will be removed.<p>
 * <p/>
 * <p>Usage:<p/>
 * <pre>
 * BeanManager bm = BeanManagerProvider.getInstance().getBeanManager();
 * 
 * </pre>
 *
 * <p><b>Attention:</b> This method is intended for being used in user code at runtime.
 * If this method gets used during Container boot (in an Extension), non-portable
 * behaviour results. During bootstrapping an Extension shall &#064;Inject BeanManager to get
 * access to the underlying BeanManager (see e.g. {@link #cleanupFinalBeanManagers} ).
 * This is the only way to guarantee to get the right
 * BeanManager in more complex Container scenarios.</p>
 */
public class BeanManagerProvider implements Extension
{
    private static final Logger  LOG = Logger.getLogger(BeanManagerProvider.class.getName());

    private static BeanManagerProvider bmpSingleton = null;

    /**
     * This data container is used for storing the BeanManager for each
     * WebApplication. This is needed in EAR or other multi-webapp scenarios
     * if the DeltaSpike classes (jars) are provided in a shared ClassLoader.
     */
    private static class BeanManagerInfo
    {
        /**
         * The BeanManager picked up via Extension loading
         */
        private BeanManager loadTimeBm = null;

        /**
         * The final BeanManagers.
         * After the container did finally boot, we first try to resolve them from JNDI,
         * and only if we don't find any BM there we take the ones picked up at startup.
         */
        private BeanManager finalBm = null;

        /**
         * Whether the CDI Application has finally booted.
         * Please note that this is only a nearby value
         * as there is no reliable event for this status in EE6.
         */
        private boolean booted = false;
    }

    /**
     * <p>The BeanManagerInfo for the current ClassLoader.</p>
     * <p><b>Attention:</b> This instance must only be used through the {@link #bmpSingleton} singleton!</p>
     */
    private volatile Map<ClassLoader, BeanManagerInfo> bmInfos = new ConcurrentHashMap<ClassLoader, BeanManagerInfo>();

    /**
     * Returns if the {@link BeanManagerProvider} has been initialized.
     * Usually it isn't needed to call this method in application code.
     * It's e.g. useful for other frameworks to check if DeltaSpike and the CDI container in general have been started.
     *
     * @return true if the bean-manager-provider is ready to be used
     */
    public static boolean isActive()
    {
        return bmpSingleton != null;
    }

    /**
     * Allows to get the current provider instance which provides access to the current {@link BeanManager}
     *
     * @throws IllegalStateException if the {@link BeanManagerProvider} isn't ready to be used.
     * That's the case if the environment isn't configured properly and therefore the {@link AfterBeanDiscovery}
     * hasn't be called before this method gets called.
     * @return the singleton BeanManagerProvider
     */
    public static BeanManagerProvider getInstance()
    {
        /*X TODO Java-EE5 support needs to be discussed
        if (bmpSingleton == null)
        {
            // workaround for some Java-EE5 environments in combination with a special
            // StartupBroadcaster for bootstrapping CDI

            // CodiStartupBroadcaster.broadcastStartup();
            // here bmp might not be null (depends on the broadcasters)
        }
        */

        if (bmpSingleton == null)
        {
            throw new IllegalStateException("No " + BeanManagerProvider.class.getName() + " in place! " +
                    "Please ensure that you configured the CDI implementation of your choice properly. " +
                    "If your setup is correct, please clear all caches and compiled artifacts.");
        }
        return bmpSingleton;
    }

    /**
     * It basically doesn't matter which of the system events we use,
     * but basically we use the {@link AfterBeanDiscovery} event since it allows to use the
     * {@link BeanManagerProvider} for all events which occur after the {@link AfterBeanDiscovery} event.
     *
     * @param afterBeanDiscovery event which we don't actually use ;)
     * @param beanManager        the BeanManager we store and make available.
     */
    public void setBeanManager(@Observes AfterBeanDiscovery afterBeanDiscovery, BeanManager beanManager)
    {
        setBeanManagerProvider(this);

        BeanManagerInfo bmi = getBeanManagerInfo(getClassLoader(null));
        bmi.loadTimeBm =  beanManager;
    }

    /**
     * The active {@link BeanManager} for the current application (/{@link ClassLoader}). This method will throw an
     * {@link IllegalStateException} if the BeanManager cannot be found.
     *
     * @return the current bean-manager, never <code>null</code>
     * @throws IllegalStateException if the BeanManager cannot be found
     */
    public BeanManager getBeanManager()
    {
        BeanManagerInfo bmi = getBeanManagerInfo(getClassLoader(null));

        // warn the user if he tries to use the BeanManager before container startup
        if (!bmi.booted)
        {
            LOG.warning("When using the BeanManager to retrieve Beans before the Container is started," +
                    " non-portable behaviour results!");
        }

        BeanManager result = bmi.finalBm;

        if (result == null)
        {
            synchronized (this)
            {
                result = bmi.finalBm;
                if (result == null)
                {
                    // first we look for a BeanManager from JNDI
                    result = resolveBeanManagerViaJndi();

                    if (result == null)
                    {
                        // if none found, we take the one we got from the Extension loading
                        result = bmi.loadTimeBm;
                    }

                    if (result == null)
                    {
                        throw new IllegalStateException("Unable to find BeanManager. " +
                                "Please ensure that you configured the CDI implementation of your choice properly.");
                    }

                    // store the resolved BeanManager in the result cache until #cleanupFinalBeanManagers gets called
                    // -> afterwards the next call of #getBeanManager will trigger the final lookup
                    bmi.finalBm = result;
                }
            }
        }

        return result;
    }    /**
     * Detect the right ClassLoader.
     * The lookup order is determined by:
     * <ol>
     * <li>ContextClassLoader of the current Thread</li>
     * <li>ClassLoader of the given Object 'o'</li>
     * <li>ClassLoader of this very ClassUtils class</li>
     * </ol>
     *
     * @param o if not <code>null</code> it may get used to detect the classloader.
     * @return The {@link ClassLoader} which should get used to create new instances
     */
    public static ClassLoader getClassLoader(Object o)
    {
        if (System.getSecurityManager() != null)
        {
            return AccessController.doPrivileged(new GetClassLoaderAction(o));
        }
        else
        {
            return getClassLoaderInternal(o);
        }
    }   
    static class GetClassLoaderAction implements PrivilegedAction<ClassLoader>
    {
        private Object object;
        GetClassLoaderAction(Object object)
        {
            this.object = object;
        }

        @Override
        public ClassLoader run()
        {
            try
            {
                return getClassLoaderInternal(object);
            }
            catch (Exception e)
            {
                return null;
            }
        }
    }

    private static ClassLoader getClassLoaderInternal(Object o)
    {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        if (loader == null && o != null)
        {
            loader = o.getClass().getClassLoader();
        }

        if (loader == null)
        {
            loader = BeanManagerProvider.class.getClassLoader();
        }

        return loader;
    }

    /**
     * By cleaning the final BeanManager map after the Deployment got Validated,
     * we prevent premature loading of information from JNDI in cases where the
     * container might not be fully setup yet.
     *
     * This might happen if someone uses the BeanManagerProvider during Extension
     * startup.
     */
    public void cleanupFinalBeanManagers(@Observes AfterDeploymentValidation adv)
    {
        for (BeanManagerInfo bmi : bmpSingleton.bmInfos.values())
        {
            bmi.finalBm = null;
            bmi.booted = true;

            /*possible issue with >weld< based servers:
            if #getBeanManager gets called in a custom AfterDeploymentValidation observer >after< this observer,
            the wrong bean-manager might get stored (not deterministic due to the unspecified order of observers).
            finally a bean-manager for a single bda will be stored and returned (which isn't the bm exposed via jndi).*/
        }
    }

    /**
     * Cleanup on container shutdown
     *
     * @param beforeShutdown cdi shutdown event
     */
    public void cleanupStoredBeanManagerOnShutdown(@Observes BeforeShutdown beforeShutdown)
    {
        if (bmpSingleton == null)
        {
            // this happens if there has been a failure at startup
            return;
        }

        ClassLoader classLoader = getClassLoader(null);
        bmpSingleton.bmInfos.remove(classLoader);

        //X TODO this might not be enough as there might be
        //X ClassLoaders used during Weld startup which are not the TCCL...
    }

    /**
     * Get the BeanManager from the JNDI registry.
     *
     * @return current {@link BeanManager} which is provided via JNDI
     */
    private BeanManager resolveBeanManagerViaJndi()
    {
        try
        {
            // this location is specified in JSR-299 and must be
            // supported in all certified EE environments
            return (BeanManager) new InitialContext().lookup("java:comp/BeanManager");
        }
        catch (NamingException e)
        {
            //workaround didn't work -> return null
            return null;
        }
    }

    /**
     * Get or create the BeanManagerInfo for the given ClassLoader
     */
    private BeanManagerInfo getBeanManagerInfo(ClassLoader cl)
    {
        BeanManagerInfo bmi = bmpSingleton.bmInfos.get(cl);

        if (bmi == null)
        {
            synchronized (this)
            {
                bmi = bmpSingleton.bmInfos.get(cl);
                if (bmi == null)
                {
                    bmi = new BeanManagerInfo();
                    bmpSingleton.bmInfos.put(cl, bmi);
                }
            }
        }

        return bmi;
    }

    /**
     * This function exists to prevent findbugs to complain about
     * setting a static member from a non-static function.
     *
     * @param beanManagerProvider the bean-manager-provider which should be used if there isn't an existing provider
     * @return the first BeanManagerProvider
     */
    private static BeanManagerProvider setBeanManagerProvider(BeanManagerProvider beanManagerProvider)
    {
        if (bmpSingleton == null)
        {
            bmpSingleton = beanManagerProvider;
        }

        return bmpSingleton;
    }
}
