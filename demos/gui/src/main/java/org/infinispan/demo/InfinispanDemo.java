package org.infinispan.demo;

import org.infinispan.Cache;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.*;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachemanagerlistener.annotation.*;
import org.infinispan.notifications.cachemanagerlistener.event.*;
import org.infinispan.remoting.transport.Address;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.LegacyKeySupportSystemProperties;
import org.infinispan.commons.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.swing.*;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellRenderer;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Run it with -Djgroups.bind_addr=127.0.0.1 -Djava.net.preferIPv4Stack=true
 *
 * @author Manik Surtani
 */
public class InfinispanDemo {
   private static final Log log = LogFactory.getLog(InfinispanDemo.class);
   private static JFrame frame;
   private JTabbedPane mainPane;
   private JPanel panel1;
   private JLabel cacheStatus;
   private JPanel dataGeneratorTab;
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
   private JButton randomGeneratorButton;
   private JButton cacheClearButton;
   private JTextArea configFileContents;
   private String cacheConfigFile;
   private Cache<String, String> cache;
   private String startCacheButtonLabel = "Start Cache", stopCacheButtonLabel = "Stop Cache";
   private String statusStarting = "Starting Cache ... ", statusStarted = "Cache Running.", statusStopping = "Stopping Cache ...", statusStopped = "Cache Stopped.";
   private ExecutorService asyncExecutor;
   private ExecutorService tableUpdateExecutor;
   private JTable dataTable;
   private JSlider generateSlider;
   private JSpinner lifespanSpinner;
   private JSpinner maxIdleSpinner;
   private JButton refreshButton;
   private JPanel dataViewControlPanel;
   private JLabel cacheContentsSizeLabel;
   private Random r = new Random();
   private ClusterTableModel clusterTableModel;
   private CachedDataTableModel cachedDataTableModel;
   private DefaultCacheManager cacheManager;

   public static void main(String[] args) {
      String cfgFileName = null;
      if(args.length > 0)
         cfgFileName=args[0];
      else
         cfgFileName = LegacyKeySupportSystemProperties.getProperty("infinispan.configuration", "infinispan.demo.cfg", "config-samples/gui-demo-cache-config.xml");
      frame = new JFrame("Infinispan GUI Demo (STOPPED)");
      frame.setContentPane(new InfinispanDemo(cfgFileName).panel1);
      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      frame.pack();
      frame.setVisible(true);
      frame.setResizable(true);
   }

   public InfinispanDemo(String cfgFileName) {
      asyncExecutor = Executors.newFixedThreadPool(1);
      tableUpdateExecutor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue(1),
            new ThreadPoolExecutor.DiscardPolicy());

      cacheConfigFile = cfgFileName;
      cacheStatusProgressBar.setVisible(false);
      cacheStatusProgressBar.setEnabled(false);
      configFileName.setText(cacheConfigFile);

      // data tables
      clusterTableModel = new ClusterTableModel();
      clusterTable.setModel(clusterTableModel);
      cachedDataTableModel = new CachedDataTableModel();
      dataTable.setModel(cachedDataTableModel);

      // default state of the action button should be unstarted.
      actionButton.setText(startCacheButtonLabel);
      cacheStatus.setText(statusStopped);

      // when we start up scan the classpath for a file named
      actionButton.addActionListener(new ActionListener() {
         @Override
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
         @Override
         public void actionPerformed(ActionEvent e) {
            processAction(goButton, true);

            // do this in a separate thread
            asyncExecutor.execute(new Runnable() {
               @Override
               public void run() {
                  // based on the value of the radio button:
                  try {
                     if (putEntryRadioButton.isSelected()) {
                        cache.put(keyTextField.getText(), valueTextField.getText(), lifespan(), TimeUnit.MILLISECONDS, maxIdle(), TimeUnit.MILLISECONDS);
                     } else if (removeEntryRadioButton.isSelected()) {
                        cache.remove(keyTextField.getText());
                     } else if (getEntryRadioButton.isSelected()) {
                        cache.get(keyTextField.getText());
                     }
                  }
                  catch(Throwable t) {
                     // log.error("failed to update cache", t);
                  }
                  finally {
                     dataViewTab.repaint();
                     processAction(goButton, false);
                  }

                  // reset these values
                  lifespanSpinner.setValue(cache.getCacheConfiguration().expiration().lifespan());
                  maxIdleSpinner.setValue(cache.getCacheConfiguration().expiration().maxIdle());
                  // now switch to the data pane
                  mainPane.setSelectedIndex(1);
               }

               private long lifespan() {
                  try {
                     String s = lifespanSpinner.getValue().toString();
                     return Long.parseLong(s);
                  } catch (Exception e) {
                     return cache.getCacheConfiguration().expiration().lifespan();
                  }
               }

               private long maxIdle() {
                  try {
                     String s = maxIdleSpinner.getValue().toString();
                     return Long.parseLong(s);
                  } catch (Exception e) {
                     return cache.getCacheConfiguration().expiration().maxIdle();
                  }
               }
            });
         }
      });

      removeEntryRadioButton.addActionListener(new ActionListener() {
         @Override
         public void actionPerformed(ActionEvent e) {
            keyTextField.setEnabled(true);
            valueTextField.setEnabled(false);
            lifespanSpinner.setEnabled(false);
            maxIdleSpinner.setEnabled(false);
         }
      });

      putEntryRadioButton.addActionListener(new ActionListener() {
         @Override
         public void actionPerformed(ActionEvent e) {
            keyTextField.setEnabled(true);
            valueTextField.setEnabled(true);
            lifespanSpinner.setEnabled(true);
            maxIdleSpinner.setEnabled(true);
         }
      });

      getEntryRadioButton.addActionListener(new ActionListener() {
         @Override
         public void actionPerformed(ActionEvent e) {
            keyTextField.setEnabled(true);
            valueTextField.setEnabled(false);
            lifespanSpinner.setEnabled(false);
            maxIdleSpinner.setEnabled(false);
         }
      });

      generateSlider.addChangeListener(new ChangeListener() {

         @Override
         public void stateChanged(ChangeEvent e) {
            randomGeneratorButton.setText("Generate " + generateSlider.getValue() + " Random Entries");
         }
      });

      randomGeneratorButton.setText("Generate " + generateSlider.getValue() + " Random Entries");

      randomGeneratorButton.addActionListener(new ActionListener() {
         @Override
         public void actionPerformed(ActionEvent e) {
            processAction(randomGeneratorButton, true);

            // process this asynchronously
            asyncExecutor.execute(new Runnable() {
               @Override
               public void run() {
                  int entries = generateSlider.getValue();

                  if (entries > 1000) {
                     for (int i = 0; i < entries / 1000; i++) {
                        Map<String, String> rand = new HashMap<String, String>();
                        while (rand.size() < 1000) rand.put(randomString(), randomString());
                        cache.putAll(rand);
                     }
                     // generate the rest of the entries
                     entries = entries % 1000;
                  }

                  Map<String, String> rand = new HashMap<String, String>();
                  while (rand.size() < entries) rand.put(randomString(), randomString());
                  cache.putAll(rand);

                  processAction(randomGeneratorButton, false);
                  generateSlider.setValue(50);
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
         @Override
         public void actionPerformed(ActionEvent e) {
            processAction(cacheClearButton, true);
            asyncExecutor.execute(new Runnable() {
               @Override
               public void run() {
                  cache.clear();
                  processAction(cacheClearButton, false);
                  // now switch to the data pane
                  mainPane.setSelectedIndex(1);
               }
            });
         }
      });

      refreshButton.addActionListener(new ActionListener() {
         @Override
         public void actionPerformed(ActionEvent e) {
            processAction(refreshButton, true);
            asyncExecutor.execute(new Runnable() {
               @Override
               public void run() {
                  InfinispanDemo.this.updateCachedDataTable();
                  processAction(refreshButton, false);
                  // now switch to the data pane
                  mainPane.setSelectedIndex(1);
               }
            });
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
         @Override
         public void run() {
            try {
               URL resource = new FileLookup().lookupFileLocation(cacheConfigFile, getClass().getClassLoader());
               if (resource == null) resource = new URL(cacheConfigFile);

               if (cacheManager == null) {
                  // update config file display
                  InputStream stream = resource.openStream();
                  try {
                     cacheManager = new DefaultCacheManager(stream);
                  } finally {
                     Util.close(stream);
                  }
               } 
               cache = cacheManager.getCache();
               cache.start();

               // repaint the cfg file display
               configFileName.setText(resource.toString());
               configFileName.repaint();

               InputStream is = null;
               try {
                  is = resource.openStream();
                  configFileContents.setText(readContents(is));
                  configFileContents.setEditable(false);
               }
               catch (Exception e) {
                  log.warn("Unable to open config file [" + cacheConfigFile + "] for display", e);
               } finally {
                  Util.close(is);
               }
               configFileContents.repaint();


               CacheListener cl = new CacheListener();
               cache.addListener(cl);
               EmbeddedCacheManager cacheManager = cache.getCacheManager();
               cacheManager.addListener(cl);
               updateClusterTable(cacheManager.getMembers());

               lifespanSpinner.setValue(cache.getCacheConfiguration().expiration().lifespan());
               maxIdleSpinner.setValue(cache.getCacheConfiguration().expiration().maxIdle());
               cacheContentsSizeLabel.setText("Cache contains " + cache.size() + " entries");

               moveCacheToState(ComponentStatus.RUNNING);
            } catch (Exception e) {
               log.error("Unable to start cache!", e);
               throw new RuntimeException(e);
            }
         }
      });
   }

   private void stopCache() {
      moveCacheToState(ComponentStatus.STOPPING);
      // actually stop the cache asynchronously
      asyncExecutor.execute(new Runnable() {
         @Override
         public void run() {
            if (cache != null) {
               cache.stop();
               cache = null;
            }
            if (cacheManager != null) {
               cacheManager.stop();
               cacheManager = null;
            }
            cachedDataTableModel.reset();
            configFileContents.setText("");
            configFileContents.repaint();
            configFileName.setText("");
            configFileName.repaint();
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
      clusterTableModel.setMembers(members);
      updateTitleBar();
   }

   private void updateTitleBar() {
      String title = "Infinispan GUI Demo";
      if (cache != null && cache.getStatus() == ComponentStatus.RUNNING) {
         title += " (STARTED) " + getLocalAddress() + " Cluster size: " + getClusterSize();
      } else {
         title += " (STOPPED)";
      }
      frame.setTitle(title);
   }

   private String getLocalAddress() {
      EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) cache.getCacheManager();
      Address a = cacheManager.getAddress();
      if (a == null) return "(LOCAL mode)";
      else return a.toString();
   }

   private String getClusterSize() {
      EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) cache.getCacheManager();
      List<Address> members = cacheManager.getMembers();
      return members == null || members.isEmpty() ? "N/A" : "" + members.size();
   }

   private void createUIComponents() {
      dataTable = new AlternateColorTable();

   }

   public static class AlternateColorTable extends JTable {

      final Color c1 = new Color(0xEE, 0xEE, 0xEE);
      final Color c2 = new Color(0xFF, 0xFF, 0xEE);

      /**
       * Returns the appropriate background color for the given row.
       */
      protected Color colorForRow(int row) {
         return (row % 2 == 0) ? c1 : c2;
      }

      /**
       * Shades alternate rows in different colors.
       */
      @Override
      public Component prepareRenderer(TableCellRenderer renderer, int row, int column) {
         Component c = super.prepareRenderer(renderer, row, column);
         if (!isCellSelected(row, column)) {
            c.setBackground(colorForRow(row));
            c.setForeground(UIManager.getColor("Table.foreground"));
         } else {
            c.setBackground(UIManager.getColor("Table.selectionBackground"));
            c.setForeground(UIManager.getColor("Table.selectionForeground"));
         }
         return c;
      }
   }

   @Listener(sync = true)
   public class CacheListener {
      @ViewChanged
      @Merged
      public void viewChangeEvent(ViewChangedEvent e) {
         updateClusterTable(e.getNewMembers());
      }

      @CacheEntryModified
      @CacheEntryRemoved
      @CacheEntriesEvicted
      public void removed(Event<?, ?> e) {
         if (!e.isPre()) updateCachedDataTable();
      }
   }

   private void updateCachedDataTable() {
      tableUpdateExecutor.execute(new Runnable() {
         @Override
         public void run() {
            cachedDataTableModel.update();
         }
      });
   }

   public class ClusterTableModel extends AbstractTableModel {
      List<String> members = new ArrayList<String>();
      List<String> memberStates = new ArrayList<String>();
      private static final long serialVersionUID = -4321027648450429007L;

      public void setMembers(List<Address> m) {
         if (m != null && !m.isEmpty()) {
            members = new ArrayList<String>(m.size());
            for (Address ma : m) members.add(ma.toString());

            memberStates = new ArrayList<String>(m.size());
            for (Address a : m) {
               String extraInfo = "Member";
               // if this is the first member then this is the coordinator
               if (memberStates.isEmpty()) extraInfo += " (coord)";
               EmbeddedCacheManager cacheManager = (EmbeddedCacheManager) cache.getCacheManager();
               if (a.equals(cacheManager.getAddress()))
                  extraInfo += " (me)";

               memberStates.add(extraInfo);
            }
         } else {
            members = Collections.singletonList("me!");
            memberStates = Collections.singletonList("(local mode)");
         }

         fireTableDataChanged();
      }

      @Override
      public int getRowCount() {
         return members.size();
      }

      @Override
      public int getColumnCount() {
         return 2;
      }

      @Override
      public Object getValueAt(int rowIndex, int columnIndex) {
         switch (columnIndex) {
            case 0:
               return members.get(rowIndex);
            case 1:
               return memberStates.get(rowIndex);
         }
         return "NULL!";
      }

      @Override
      public String getColumnName(int c) {
         if (c == 0) return "Member Address";
         if (c == 1) return "Member Info";
         return "NULL!";
      }
   }

   public class CachedDataTableModel extends AbstractTableModel {

      List<InternalCacheEntry> data = new ArrayList<InternalCacheEntry>();
      private static final long serialVersionUID = -7109980678271415778L;

      @Override
      public int getRowCount() {
         return data.size();
      }

      @Override
      public int getColumnCount() {
         return 4;
      }

      @Override
      public Object getValueAt(int rowIndex, int columnIndex) {
         if (data.size() > rowIndex) {
            InternalCacheEntry e = data.get(rowIndex);
            switch (columnIndex) {
               case 0:
                  return e.getKey();
               case 1:
                  return e.getValue();
               case 2:
                  return e.getLifespan();
               case 3:
                  return e.getMaxIdle();
            }
         }
         return "NULL!";
      }

      @Override
      public String getColumnName(int c) {
         switch (c) {
            case 0:
               return "Key";
            case 1:
               return "Value";
            case 2:
               return "Lifespan";
            case 3:
               return "MaxIdle";
         }
         return "NULL!";
      }

      public void update() {
         // whew - expensive stuff.
         data.clear();
         long currentTimeMillis = System.currentTimeMillis();
         for (InternalCacheEntry ice : cache.getAdvancedCache().getDataContainer()) {
            if (!ice.isExpired(currentTimeMillis)) data.add(ice);
         }
         cacheContentsSizeLabel.setText("Cache contains " + data.size() + " entries");
         fireTableDataChanged();
      }

      public void reset() {
         data.clear();
         cacheContentsSizeLabel.setText("Cache contains " + data.size() + " entries");
         fireTableDataChanged();
      }
   }

   class CachedEntry {
      String key, value;
      long lifespan = -1, maxIdle = -1;

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         CachedEntry that = (CachedEntry) o;

         if (lifespan != that.lifespan) return false;
         if (maxIdle != that.maxIdle) return false;
         if (key != null ? !key.equals(that.key) : that.key != null) return false;
         if (value != null ? !value.equals(that.value) : that.value != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = key != null ? key.hashCode() : 0;
         result = 31 * result + (value != null ? value.hashCode() : 0);
         result = 31 * result + (int) (lifespan ^ (lifespan >>> 32));
         result = 31 * result + (int) (maxIdle ^ (maxIdle >>> 32));
         return result;
      }
   }
}
