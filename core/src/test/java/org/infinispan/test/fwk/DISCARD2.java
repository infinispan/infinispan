
package org.infinispan.test.fwk;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.annotations.Property;
import org.jgroups.annotations.Unsupported;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


/**
 * Copied from {@link org.jgroups.protocols.DISCARD}, because of some concurrency problems in there
 * (to be fixed in the next release).
 */
@Unsupported
@MBean(description="Discards messages")
public class DISCARD2 extends Protocol {
    @Property
    protected double                    up=0.0;    // probability of dropping up   msgs

    @Property
    protected double                    down=0.0;  // probability of dropping down msgs

    @Property
    protected boolean                   excludeItself=true;   // if true don't discard messages sent/received in this stack
    protected Address                   localAddress;

    @ManagedAttribute(description="Number of dropped down messages",name="dropped_down_messages")
    protected int                       num_down=0;

    @ManagedAttribute(description="Number of dropped up messages",name="dropped_up_messages")
    protected int                       num_up=0;
    
    protected final Set<Address>        ignoredMembers = Collections.synchronizedSet(new HashSet<Address>());


    protected final Collection<Address> members= Collections.synchronizedList(new ArrayList<Address>());

    @Property(description="drop all messages (up or down)",writable=true)
    protected boolean                   discard_all=false;

    @Property(description="Number of subsequent unicasts to drop in the down direction",writable=true)
    protected int                       drop_down_unicasts=0;

    @Property(description="Number of subsequent multicasts to drop in the down direction",writable=true)
    protected int                       drop_down_multicasts=0;

    protected DiscardDialog             discard_dialog=null;

    @Property(name="gui", description="use a GUI or not")
    protected boolean                   use_gui=false;


    public DISCARD2 localAddress(Address addr) {setLocalAddress(addr); return this;}

    public Address                      localAddress() {
        if(localAddress == null)
            localAddress=(Address)up_prot.up(new Event(Event.GET_LOCAL_ADDRESS));
        return localAddress;
    }

    public boolean isDiscardAll() {
        return discard_all;
    }

    public void setDiscardAll(boolean discard_all) {
        this.discard_all=discard_all;
    }

    public boolean isExcludeItself() {
        return excludeItself;
    }
    
    public void setLocalAddress(Address localAddress){
    	this.localAddress =localAddress;
        if(discard_dialog != null)
            discard_dialog.setTitle(localAddress != null? localAddress.toString() : "n/a");
    }

    public void setExcludeItself(boolean excludeItself) {
        this.excludeItself=excludeItself;
    }

    public double getUpDiscardRate() {
        return up;
    }

    public void setUpDiscardRate(double up) {
        this.up=up;
    }

    public double getDownDiscardRate() {
        return down;
    }

    public void setDownDiscardRate(double down) {
        this.down=down;
    }

    public int getDropDownUnicasts() {
        return drop_down_unicasts;
    }

    /**
     * Drop the next N unicasts down the stack
     * @param drop_down_unicasts
     */
    public void setDropDownUnicasts(int drop_down_unicasts) {
        this.drop_down_unicasts=drop_down_unicasts;
    }

    public int getDropDownMulticasts() {
        return drop_down_multicasts;
    }

    public void setDropDownMulticasts(int drop_down_multicasts) {
        this.drop_down_multicasts=drop_down_multicasts;
    }

    /** Messages from this sender will get dropped */
    public void addIgnoreMember(Address sender) {ignoredMembers.add(sender);}

    public void removeIgnoredMember(Address member) {ignoredMembers.remove(member);}

    public void resetIgnoredMembers() {ignoredMembers.clear();}


    @ManagedOperation
    public void startGui() {
        if(discard_dialog == null) {
            discard_dialog=new DiscardDialog();
            discard_dialog.init();
            discard_dialog.setTitle(localAddress() != null? localAddress().toString() : "n/a");
            discard_dialog.handleView(members);
        }
    }

    @ManagedOperation
    public void stopGui() {
        if(discard_dialog != null)
            discard_dialog.dispose();
        discard_dialog=null;
    }

    public void start() throws Exception {
        super.start();
        if(use_gui) {
            discard_dialog=new DiscardDialog();
            discard_dialog.init();
        }
    }

    public void stop() {
        super.stop();
        if(discard_dialog != null)
            discard_dialog.dispose();
    }

    public Object up(Event evt) {
        if(evt.getType() == Event.SET_LOCAL_ADDRESS) {
            localAddress=(Address)evt.getArg();
            if(discard_dialog != null)
                discard_dialog.setTitle("Discard dialog (" + localAddress + ")");
        }

        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            if(shouldDropUpMessage(msg, msg.getSrc()))
                return null;
        }

        return up_prot.up(evt);
    }


    public void up(MessageBatch batch) {
        for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
            Message msg=it.next();
            if(msg != null && shouldDropUpMessage(msg, msg.getSrc()))
                it.remove();
        }
        if(!batch.isEmpty())
            up_prot.up(batch);
    }


    public Object down(Event evt) {
        Message msg;
        double r;

        switch(evt.getType()) {
            case Event.MSG:
                msg=(Message)evt.getArg();
                Address dest=msg.getDest();
                boolean multicast=dest == null;

                if(msg.getSrc() == null)
                    msg.setSrc(localAddress());

                if(discard_all) {
                    if(dest == null || dest.equals(localAddress()))
                        loopback(msg);
                    return null;
                }

                if(!multicast && drop_down_unicasts > 0) {
                    drop_down_unicasts=Math.max(0, drop_down_unicasts -1);
                    return null;
                }

                if(multicast && drop_down_multicasts > 0) {
                    drop_down_multicasts=Math.max(0, drop_down_multicasts -1);
                    return null;
                }

                if(down > 0) {
                    r=Math.random();
                    if(r < down) {
                        if(excludeItself && dest != null && dest.equals(localAddress())) {
                            if(log.isTraceEnabled()) log.trace("excluding itself");
                        }
                        else {
                            log.trace("dropping message");
                            num_down++;
                            return null;
                        }
                    }
                }
                break;
            case Event.VIEW_CHANGE:
                View view=(View)evt.getArg();
                List<Address> mbrs=view.getMembers();
                members.clear();
                members.addAll(mbrs);
//                ignoredMembers.retainAll(mbrs); // remove all non members
                if(discard_dialog != null)
                    discard_dialog.handleView(mbrs);
                break;

            case Event.SET_LOCAL_ADDRESS:
                localAddress=(Address)evt.getArg();
                if(discard_dialog != null)
                    discard_dialog.setTitle("Discard dialog (" + localAddress + ")");
                break;
            case Event.GET_PING_DATA:
                if(discard_all)
                    return null;
                break;
        }

        return down_prot.down(evt);
    }


    /** Checks if a message should be passed up, or not */
    protected boolean shouldDropUpMessage(Message msg, Address sender) {
        if(discard_all && !sender.equals(localAddress()))
            return true;

        if(ignoredMembers.contains(sender)) {
            if(log.isTraceEnabled())
                log.trace(localAddress + ": dropping message from " + sender);
            num_up++;
            return true;
        }

        if(up > 0) {
            double r=Math.random();
            if(r < up) {
                if(excludeItself && sender.equals(localAddress())) {
                    if(log.isTraceEnabled())
                        log.trace("excluding myself");
                }
                else {
                    if(log.isTraceEnabled())
                        log.trace(localAddress + ": dropping message from " + sender);
                    num_up++;
                    return true;
                }
            }
        }

        return false;
    }


    private void loopback(Message msg) {
        final Message rsp=msg.copy(true);
        if(rsp.getSrc() == null)
            rsp.setSrc(localAddress());

        // pretty inefficient: creates one thread per message, okay for testing only
        Thread thread=new Thread(new Runnable() {
            public void run() {
                up_prot.up(new Event(Event.MSG, rsp));
            }
        });
        thread.start();
    }

    public void resetStats() {
        super.resetStats();
        num_down=num_up=0;
    }


    


    protected class DiscardDialog extends JFrame implements ActionListener {
        private JButton start_discarding_button=new JButton("start discarding");
        private JButton stop_discarding_button=new JButton("stop discarding");
        JPanel checkboxes=new JPanel();


        protected DiscardDialog() {
        }

        void init() {
            getContentPane().setLayout(new BoxLayout(getContentPane(), BoxLayout.Y_AXIS));
            checkboxes.setLayout(new BoxLayout(checkboxes, BoxLayout.Y_AXIS));
            getContentPane().add(start_discarding_button);
            getContentPane().add(stop_discarding_button);
            start_discarding_button.addActionListener(this);
            stop_discarding_button.addActionListener(this);
            getContentPane().add(checkboxes);
            pack();
            setVisible(true);
            setTitle(localAddress() != null? localAddress().toString() : "n/a");
        }


        public void actionPerformed(ActionEvent e) {
            String command=e.getActionCommand();
            if(command.startsWith("start")) {
                discard_all=true;
            }
            else if(command.startsWith("stop")) {
                discard_all=false;
                Component[] comps=checkboxes.getComponents();
                for(Component c: comps) {
                    if(c instanceof JCheckBox) {
                        ((JCheckBox)c).setSelected(false);
                    }
                }
            }
        }

        void handleView(Collection<Address> mbrs) {
            checkboxes.removeAll();
            for(final Address addr: mbrs) {
                final MyCheckBox box=new MyCheckBox("discard traffic from " + addr, addr);
                box.addActionListener(new ActionListener() {
                    public void actionPerformed(ActionEvent e) {
                        if(box.isSelected()) {
                            ignoredMembers.add(addr);
                        }
                        else {
                            ignoredMembers.remove(addr);
                        }
                    }
                });
                checkboxes.add(box);
            }

            for(Component comp: checkboxes.getComponents()) {
                MyCheckBox box=(MyCheckBox)comp;
                if(ignoredMembers.contains(box.mbr))
                    box.setSelected(true);
            }
            pack();
        }
    }

    private static class MyCheckBox extends JCheckBox {
        final Address mbr;

        public MyCheckBox(String name, Address member) {
            super(name);
            this.mbr=member;
        }

        public String toString() {
            return super.toString() + " [mbr=" + mbr + "]";
        }
    }
}