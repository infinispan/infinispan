package org.infinispan.demo;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.infinispan.Cache;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.jgroups.Address;

import javax.swing.*;
import javax.swing.table.AbstractTableModel;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Manik Surtani
 */
public class InfinispanDemo {
   private static Log log = LogFactory.getLog(InfinispanDemo.class);
   private static JFrame frame;
   private JTabbedPane mainPane;
   private JPanel panel1;
   private JLabel cacheStatus;
   private JPanel dataGeneratorTab;
   private JPanel statisticsTab;
   private JPanel clusterViewTab;
   private JPanel dataViewTab;
   private JPanel controlPanelTab;
   private JTable clusterTable;
   private JButton actionButton;
   private JLabel configFileName;
   private JProgressBar cacheStatusProgressBar;
   private JTextField keyTextField;
   private JTextField valueTextField;
   private JRadioButton putEntryRadioButton;
   private JRadioButton removeEntryRadioButton;
   private JRadioButton getEntryRadioButton;
   private JButton goButton;
   private JScrollPane nodeDataScrollPane;
   private JButton randomGeneratorButton;
   private JTextField maxEntriesTextField;
   private JButton cacheClearButton;
   private JTextArea configFileContents;
   private JScrollPane treeScrollPane;
   private JPanel debugTab;
   private JButton cacheDetailsButton;
   private JButton cacheLockInfoButton;
   private JTextArea debugTextArea;
   private String cacheConfigFile;
   private Cache<String, String> cache;
   private String startCacheButtonLabel = "Start Cache", stopCacheButtonLabel = "Stop Cache";
   private String statusStarting = "Starting Cache ... ", statusStarted = "Cache Running.", statusStopping = "Stopping Cache ...", statusStopped = "Cache Stopped.";
   private ExecutorService asyncExecutor;
   private BlockingQueue<Runnable> asyncTaskQueue;
   private JTable dataTable;
   private Random r = new Random();

   public static void main(String[] args) {
      String cfgFileName = "demo-cache-config.xml";
      if (args.length == 1 && args[0] != null && args[0].toLowerCase().endsWith(".xml")) {
         // the first arg is the name of the config file.
         cfgFileName = args[0];
      }

      frame = new JFrame("Infinispan GUI Demo (STOPPED)");
      frame.setContentPane(new InfinispanDemo(cfgFileName).panel1);
      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      frame.pack();
      frame.setVisible(true);
      frame.setResizable(true);
   }

   public InfinispanDemo(String cfgFileName) {
      asyncExecutor = Executors.newFixedThreadPool(1);
      asyncTaskQueue = ((ThreadPoolExecutor) asyncExecutor).getQueue();

      cacheConfigFile = cfgFileName;
      cacheStatusProgressBar.setVisible(false);
      cacheStatusProgressBar.setEnabled(false);
      configFileName.setText(cacheConfigFile);
      // default state of the action button should be unstarted.
      actionButton.setText(startCacheButtonLabel);
      cacheStatus.setText(statusStopped);

      // when we start up scan the classpath for a file named
      actionButton.addActionListener(new ActionListener() {
         public void actionPerformed(ActionEvent e) {
            if (actionButton.getText().equals(startCacheButtonLabel)) {
               // start cache
               startCache();
            } else if (actionButton.getText().equals(stopCacheButtonLabel)) {
               // stop cache
               stopCache();
            }
         }
      });

      goButton.addActionListener(new ActionListener() {
         public void actionPerformed(ActionEvent e) {
            processAction(goButton, true);

            // do this in a separate thread
            asyncExecutor.execute(new Runnable() {
               public void run() {
                  // based on the value of the radio button:
                  if (putEntryRadioButton.isSelected()) {
                     cache.put(keyTextField.getText(), valueTextField.getText());
                  } else if (removeEntryRadioButton.isSelected()) {
                     cache.remove(keyTextField.getText());
                  } else if (getEntryRadioButton.isSelected()) {
                     cache.get(keyTextField.getText());
                  }
                  dataViewTab.repaint();
                  processAction(goButton, false);
                  // now switch to the data pane
                  mainPane.setSelectedIndex(1);
               }
            });
         }
      });
      removeEntryRadioButton.addActionListener(new ActionListener() {
         public void actionPerformed(ActionEvent e) {
            keyTextField.setEnabled(true);
            valueTextField.setEnabled(false);
         }
      });

      putEntryRadioButton.addActionListener(new ActionListener() {
         public void actionPerformed(ActionEvent e) {
            keyTextField.setEnabled(true);
            valueTextField.setEnabled(true);
         }
      });

      getEntryRadioButton.addActionListener(new ActionListener() {
         public void actionPerformed(ActionEvent e) {
            keyTextField.setEnabled(true);
            valueTextField.setEnabled(false);
         }
      });

      randomGeneratorButton.addActionListener(new ActionListener() {
         public void actionPerformed(ActionEvent e) {
            processAction(randomGeneratorButton, true);

            // process this asynchronously
            asyncExecutor.execute(new Runnable() {
               public void run() {
                  int entries = 1;
                  try {
                     entries = Integer.parseInt(maxEntriesTextField.getText());
                  }
                  catch (NumberFormatException nfe) {
                     log.warn("Entered a non-integer for depth.  Using 1.", nfe);
                  }

                  for (int i = 0; i < entries; i++) cache.put(randomString(), randomString());

                  processAction(randomGeneratorButton, false);
                  // now switch to the data pane
                  mainPane.setSelectedIndex(1);
               }
            });
         }

         private String randomString() {
            return Integer.toHexString(r.nextInt(Integer.MAX_VALUE)).toUpperCase();
         }
      });
      cacheClearButton.addActionListener(new ActionListener() {
         public void actionPerformed(ActionEvent e) {
            processAction(cacheClearButton, true);
            asyncExecutor.execute(new Runnable() {
               public void run() {
                  cache.clear();
                  processAction(cacheClearButton, false);
                  // now switch to the data pane
                  mainPane.setSelectedIndex(1);
               }
            });
         }
      });

      cacheDetailsButton.addActionListener(new ActionListener() {
         public void actionPerformed(ActionEvent e) {
            if (cache != null) debugTextArea.setText(cache.toString());
         }
      });
      cacheLockInfoButton.addActionListener(new ActionListener() {
         public void actionPerformed(ActionEvent e) {
            if (cache != null) debugTextArea.setText(cache.toString());
         }
      });
   }

   private void moveCacheToState(ComponentStatus state) {
      switch (state) {
         case INITIALIZING:
            cacheStatus.setText(statusStarting);
            processAction(actionButton, true);
            break;
         case RUNNING:
            setCacheTabsStatus(true);
            actionButton.setText(stopCacheButtonLabel);
            processAction(actionButton, false);
            cacheStatus.setText(statusStarted);
            updateTitleBar();
            break;
         case STOPPING:
            cacheStatus.setText(statusStopping);
            processAction(actionButton, true);
            break;
         case TERMINATED:
            setCacheTabsStatus(false);
            actionButton.setText(startCacheButtonLabel);
            processAction(actionButton, false);
            cacheStatus.setText(statusStopped);
            updateTitleBar();
      }
      controlPanelTab.repaint();
   }

   private void processAction(JButton button, boolean start) {
      button.setEnabled(!start);
      cacheStatusProgressBar.setVisible(start);
      cacheStatusProgressBar.setEnabled(start);
   }

   private String readContents(InputStream is) throws IOException {
      BufferedReader r = new BufferedReader(new InputStreamReader(is));
      String s;
      StringBuilder sb = new StringBuilder();
      while ((s = r.readLine()) != null) {
         sb.append(s);
         sb.append("\n");
      }
      return sb.toString();
   }

   private void startCache() {
      moveCacheToState(ComponentStatus.INITIALIZING);

      // actually start the cache asynchronously.
      asyncExecutor.execute(new Runnable() {
         public void run() {
            try {
               if (cache == null) {
                  URL resource = getClass().getClassLoader().getResource(cacheConfigFile);
                  String contents;
                  // update config file display
                  if (resource != null) {
                     configFileName.setText(resource.toString());
                  } else {
                     configFileName.setText(cacheConfigFile);
                  }
                  configFileName.repaint();

                  try {
                     configFileContents.setText(readContents(resource == null ? new FileInputStream(cacheConfigFile) : resource.openStream()));
                     configFileContents.setEditable(false);
                  }
                  catch (Exception e) {
                     log.warn("Unable to open config file for display", e);
                  }
                  configFileContents.repaint();

                  cache = new DefaultCacheManager(cacheConfigFile).getCache();
               } else {
                  cache.start();
               }

//               updateClusterTable(cache.getCacheManager().getMembers());
//               cache.addCacheListener(new CacheListener());
               moveCacheToState(ComponentStatus.RUNNING);
            } catch (Exception e) {
               log.error("Unable to start cache!", e);
            }
         }
      });
   }

   private void stopCache() {
      moveCacheToState(ComponentStatus.STOPPING);
      // actually stop the cache asynchronously
      asyncExecutor.execute(new Runnable() {
         public void run() {
            if (cache != null) cache.stop();
            moveCacheToState(ComponentStatus.TERMINATED);
         }
      });
   }

   private void setCacheTabsStatus(boolean enabled) {
      int numTabs = mainPane.getTabCount();
      for (int i = 1; i < numTabs; i++) mainPane.setEnabledAt(i, enabled);
      panel1.repaint();
   }

   private void updateClusterTable(List<Address> members) {
      log.debug("Updating cluster table with new member list " + members);
//      clusterDataModel.setMembers(members);
      updateTitleBar();
   }

   private void updateTitleBar() {
      String title = "Infinispan GUI Demo";
      if (cache != null && cache.getStatus() == ComponentStatus.RUNNING) {
         title += " (STARTED) " + cache.getCacheManager().getAddress() + " Cluster size: " + cache.getCacheManager().getMembers().size();
      } else {
         title += " (STOPPED)";
      }
      frame.setTitle(title);
   }

   @Listener
   public class CacheListener {
      @ViewChanged
      public void viewChangeEvent(ViewChangedEvent e) {
//         updateClusterTable(e.getNewView().getMembers());
      }

//      @NodeModified
//      public void nodeModified(NodeModifiedEvent e) {
//         if (!e.isPre()) {
//            // only if this is the current node selected in the tree do we bother refreshing it
//            if (nodeDataTableModel.getCurrentFqn() != null && nodeDataTableModel.getCurrentFqn().equals(e.getFqn())) {
//               nodeDataTableModel.updateCurrentNode();
//            }
//         }
//      }
//
//      @NodeCreated
//      public void nodeCreated(NodeCreatedEvent e) {
//         if (!e.isPre()) {
//            final Fqn fqn = e.getFqn();
//            asyncExecutor.execute(new Runnable() {
//               public void run() {
//                  treeRefresher.addNode(fqn);
//                  // only refresh if there are no more tasks queued up
//                  if (asyncTaskQueue.isEmpty()) treeRefresher.repaint();
//               }
//            });
//         }
//      }
//
//
//      @NodeRemoved
//      public void nodeRemoved(NodeRemovedEvent e) {
//         if (!e.isPre()) {
//            final Fqn fqn = e.getFqn();
//            asyncExecutor.execute(new Runnable() {
//               public void run() {
//                  treeRefresher.removeNode(fqn);
//                  // only refresh if there are no more tasks queued up
//                  if (asyncTaskQueue.isEmpty()) treeRefresher.repaint();
//               }
//            });
//         }
//      }

      // dont bother with node modified events since the tree GUI widget will refresh each node when it is selected.
   }

   public class ClusterTableModel extends AbstractTableModel {
      List<Address> members = new ArrayList<Address>();
      List<String> memberStates = new ArrayList<String>();

      public void setMembers(List<Address> members) {
         if (this.members != members) {
            this.members.clear();
            this.members.addAll(members);
         }

         List<Address> buddies = Collections.emptyList();

         memberStates = new ArrayList<String>(members.size());
         for (Address a : members) {
            String extraInfo = "Member";
            // if this is the first member then this is the coordinator
            if (memberStates.isEmpty()) extraInfo += " (coord)";
            if (a.equals(cache.getCacheManager().getAddress()))
               extraInfo += " (me)";
            else if (buddies.contains(a))
               extraInfo += " (buddy)";
            memberStates.add(extraInfo);
         }

         fireTableDataChanged();
      }

      public void setBuddies() {
         setMembers(members);
      }

      public int getRowCount() {
         return members.size();
      }

      public int getColumnCount() {
         return 2;
      }

      public Object getValueAt(int rowIndex, int columnIndex) {
         switch (columnIndex) {
            case 0:
               return members.get(rowIndex);
            case 1:
               return memberStates.get(rowIndex);
         }
         return "NULL!";
      }

      public String getColumnName(int c) {
         if (c == 0) return "Member Address";
         if (c == 1) return "Member Info";
         return "NULL!";
      }
   }
}
