import cluster.management.LeaderElection;
import cluster.management.OnElectionCallback;
import cluster.management.ServiceRegistry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Application implements Watcher {

    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final int DEFAULT_PORT = 8000;
    private ZooKeeper zooKeeper = null;

    public static void main(String[] args) throws InterruptedException, KeeperException, IOException {
        int currentServerPort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        Application application = new Application();
        ZooKeeper zooKeeper = application.connectToZookeeper();
        ServiceRegistry serviceRegistry = new ServiceRegistry(zooKeeper);
        OnElectionCallback onElectionCallback = new OnElectionAction(serviceRegistry, currentServerPort);
        LeaderElection leaderElection = new LeaderElection (zooKeeper, onElectionCallback);
        leaderElection.volunteerForLeadership();
        leaderElection.reelectLeader();
        application.run();
        application.close();
        System.out.println("Disconnected from ZooKeeper, Exiting");
    }
    public void run()throws InterruptedException{
        synchronized (zooKeeper){
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }


    public ZooKeeper connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        return zooKeeper;
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()){
            case None:
                if(event.getState() == Event.KeeperState.SyncConnected){
                    System.out.println("Successfully Connected to Zookeeper");
                }
                else {
                    synchronized (zooKeeper){
                        System.out.println("Disconnected From Zookeeper. exiting");
                        zooKeeper.notifyAll();
                    }
                }
        }
    }
}
