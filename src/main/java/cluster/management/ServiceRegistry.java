package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {
    private static final String REGISTRY_ZNODE = "/service_registry";
    private final ZooKeeper zooKeeper;
    private String currentZnode = null;
    private volatile List<String> allServiceAddresses =  Collections.emptyList();;
    public ServiceRegistry(ZooKeeper zooKeeper){
        this.zooKeeper = zooKeeper;
        createServiceRegistryZnode();
    }

    public void registerForUpdates(){
        try {
            updateAddresses();
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
    }
    public void unregisterFromCluster() throws InterruptedException, KeeperException {
        if(currentZnode != null && zooKeeper.exists(currentZnode, false) != null){
            zooKeeper.delete(currentZnode,-1);
        }
    }
    public synchronized List<String> getAllServiceAddresses() {
        return allServiceAddresses;
    }

    public void createServiceRegistryZnode(){

            try {
                if(zooKeeper.exists(REGISTRY_ZNODE,false) == null) {
                    zooKeeper.create(REGISTRY_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            } catch (KeeperException | InterruptedException e) {
                throw new RuntimeException(e);
            }

    }

    public void registerToCluster(String metadata){
        try {
            this.currentZnode = zooKeeper.create(REGISTRY_ZNODE+"/n_", metadata.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException  | InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Registered to Service Registry");
    }

    private synchronized void updateAddresses() throws InterruptedException, KeeperException {
        List<String> workerZnodes = zooKeeper.getChildren(REGISTRY_ZNODE, this);
        List<String> addresses = new ArrayList<>(workerZnodes.size());
        for(String workerZnode : workerZnodes){
            String workerZnodeFullPath = REGISTRY_ZNODE + "/" + workerZnode;
            Stat stat = zooKeeper.exists(workerZnodeFullPath, false);
            if(stat == null){
                continue;
            }

            byte [] addressBytes = zooKeeper.getData(workerZnodeFullPath, false, stat);
            String address = new String(addressBytes);
            addresses.add(address);
        }
        this.allServiceAddresses = Collections.unmodifiableList(addresses);
        System.out.println("The Cluster Addresses are : "+this.allServiceAddresses);
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            updateAddresses();
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
