import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

public class LeaderElector implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ELECTION_NAMESPACE = "/election";
    private static final Logger logger = Logger.getLogger("LeaderElector.class");
    private ZooKeeper zooKeeper;
    private String currentZnodeName;


    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c__";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("full path: " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void electLeader() throws KeeperException, InterruptedException {
        List<String> children=zooKeeper.getChildren(ELECTION_NAMESPACE, false);
        String smallestChild=children.get(0);
        for(String child: children){
            if(child.compareTo(smallestChild)<0)
                smallestChild=child;
        }
        if(smallestChild.equals(currentZnodeName))
            System.out.println(currentZnodeName+" is the leader.");
        else
            System.out.println(smallestChild+" is the leader.");
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
        }
    }
}
