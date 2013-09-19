package org.infinispan.atomic;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
* @author Pierre Sutra
* @since 6.0
*/
class AtomicObjectCallFuture implements Future<Object> {
    private Object ret;
    private int state; // 0 => init, 1 => done, -1 => cancelled

    public AtomicObjectCallFuture(){
        ret = null;
        state = 0;
    }

    public void setReturnValue(Object r){
        synchronized (this) {

            if (state == -1)
                return ;

            if (ret == null) {
                ret = r;
                state = 1;
                this.notifyAll();
                return;
            }
        }

        throw new IllegalStateException("Unreachable code");
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        synchronized (this) {
            if (state != 0)
                return false;
            state = -1;
            if (mayInterruptIfRunning)
                this.notifyAll();
        }
        return true;
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
        synchronized (this) {
            if (state == 0)
                this.wait();
        }
        return (state == -1) ? null : ret;
    }

    @Override
    public Object get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException,
            TimeoutException {
        synchronized (this) {
            if (state == 0)
                this.wait(timeout);
        }
        return (state == -1) ? null : ret;
    }

    @Override
    public boolean isCancelled() {
        return state == -1;
    }

    @Override
    public boolean isDone() {
        return state == 1;
    }

}
