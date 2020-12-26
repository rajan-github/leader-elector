import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger("Main.class");

    public static void main(String[] args) {
        LeaderElector leaderElection = new LeaderElector();
        try {
            leaderElection.connectToZookeeper();
            leaderElection.volunteerForLeadership();
            leaderElection.electLeader();
            leaderElection.run();
            leaderElection.close();
            logger.info("Disconnected from zookeeper. Exiting the application.");
        } catch (IOException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}
