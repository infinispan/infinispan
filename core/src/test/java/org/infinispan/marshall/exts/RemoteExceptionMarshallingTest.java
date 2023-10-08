package org.infinispan.marshall.exts;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.RemoteException;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "marshall.RemoteExceptionMarshallingTest")
public class RemoteExceptionMarshallingTest extends SingleCacheManagerTest {

    @Override
    protected EmbeddedCacheManager createCacheManager() throws Exception {
        return TestCacheManagerFactory.createCacheManager();
    }

    public void testKnownThrowable() throws Exception {
        Marshaller marshaller = TestingUtil.extractGlobalMarshaller(cacheManager);

        Throwable cause = new IllegalStateException(IllegalStateException.class.getSimpleName());
        cause.addSuppressed(new IllegalArgumentException(IllegalArgumentException.class.getSimpleName()));
        cause.addSuppressed(new IllegalStateException(IllegalStateException.class.getSimpleName()));
        cause.addSuppressed(new IllegalLifecycleStateException(IllegalLifecycleStateException.class.getSimpleName()));
        RemoteException remoteException = new RemoteException(RemoteException.class.getSimpleName(), cause);

        byte[] bytes = marshaller.objectToByteBuffer(remoteException);
        remoteException = (RemoteException) marshaller.objectFromByteBuffer(bytes);
        cause = remoteException.getCause();

        assertEquals(cause.getMessage(), IllegalStateException.class.getSimpleName());
        assertEquals(cause.getSuppressed()[0].getMessage(), IllegalArgumentException.class.getSimpleName());
        assertEquals(cause.getSuppressed()[1].getMessage(), IllegalStateException.class.getSimpleName());
        assertEquals(cause.getSuppressed()[2].getMessage(), IllegalLifecycleStateException.class.getSimpleName());
    }

    public void testGenericThrowable() throws Exception {
        Marshaller marshaller = TestingUtil.extractGlobalMarshaller(cacheManager);

        Throwable exception = new IllegalStateException(IllegalStateException.class.getSimpleName());
        exception.addSuppressed(new IllegalArgumentException(IllegalArgumentException.class.getSimpleName()));
        exception.addSuppressed(new IllegalStateException(IllegalStateException.class.getSimpleName()));

        byte[] bytes = marshaller.objectToByteBuffer(exception);
        exception = (IllegalStateException) marshaller.objectFromByteBuffer(bytes);

        assertEquals(exception.getMessage(), IllegalStateException.class.getSimpleName());
        assertEquals(exception.getSuppressed()[0].getMessage(), IllegalArgumentException.class.getSimpleName());
        assertEquals(exception.getSuppressed()[1].getMessage(), IllegalStateException.class.getSimpleName());
    }
}
