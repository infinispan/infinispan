@Start
public void init() {
   useWriteSkewCheck = configuration.locking().writeSkewCheck();
}

@Stop(priority=20)
public void stop() {
   notifier.removeListener(listener);
   executor.shutdownNow();
}
