import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import javax.swing.text.TabExpander;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

public class LeaderElector implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";
    private static final Logger logger = Logger.getLogger("LeaderElector.class");
    private static final String TARGET_ZNODE="/target_znode";
    private ZooKeeper zooKeeper;
    private String currentZnodeName;


    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c__";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("full path: " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void reelectLeader() throws KeeperException, InterruptedException {
        String predecessorZnodeName="";
        Stat predecessorStat=null;
        while(predecessorStat==null){
            List<String> children=zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild=children.get(0);
            if(smallestChild.equals(currentZnodeName))
                System.out.println(currentZnodeName+" is the leader.");
            else{
                System.out.println(smallestChild+" is the leader. And i am "+currentZnodeName);
                int predecessorIndex= Collections.binarySearch(children, currentZnodeName)-1;
                predecessorZnodeName=children.get(predecessorIndex);
                predecessorStat=zooKeeper.exists(ELECTION_NAMESPACE+"/"+predecessorZnodeName, this);
            }
        }
        System.out.println("watching: "+predecessorZnodeName);
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.close();
        }
    }

    public void connectToZookeeper() throws IOException {
        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void watchTargetZnode() throws KeeperException, InterruptedException {
        Stat stat=zooKeeper.exists(TARGET_ZNODE,this);
        if(stat==null){
            logger.info(TARGET_ZNODE+" does not exist.");
            return;
        }
        byte[] data=zooKeeper.getData(TARGET_ZNODE, this, stat);
        List<String> children=zooKeeper.getChildren(TARGET_ZNODE, this);
        System.out.println("Data: "+new String(data)+" children: "+children);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    logger.info("Successfully connected to the zookeeper.");
                } else {
                    synchronized (zooKeeper) {
                        zooKeeper.notifyAll();
                    }
                    logger.info("Connection is closed.");
                }
                break;
            case NodeDeleted:
                System.out.println(TARGET_ZNODE+" was deleted");
                try {
                    reelectLeader();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                break;
        }

        try{
            watchTargetZnode();
        }catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}
