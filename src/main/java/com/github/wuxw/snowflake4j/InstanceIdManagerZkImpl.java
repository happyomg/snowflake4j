package com.github.wuxw.snowflake4j;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

/**
 * Created by wuxinw on 2016/9/14.
 */
public class InstanceIdManagerZkImpl implements InstanceIdManager, ConnectionStateListener, Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(InstanceIdManagerZkImpl.class);
    private static final String LOCK_PATH = "/lock";
    private static final String INSTANCES_PATH = "/instances";
    private static final String INSTANCE_PATH = "instance_";
    private static final int CONFIG_WAIT_TIME = 3000;
    private final CuratorFramework client;
    private final InterProcessMutex lock;
    private final int maxNumberOfInstances;
    private String instancePath;
    private String currentInstancePath;
    private int currentId;

    public InstanceIdManagerZkImpl(String zkAddresses, String basePath, int idBits) throws Exception {
        this.currentId = -1;
        this.currentInstancePath = null;
        this.maxNumberOfInstances = (int) ~(-1L << idBits);
        this.client = CuratorFrameworkFactory.newClient(zkAddresses, new ExponentialBackoffRetry(CONFIG_WAIT_TIME, 3));
        this.client.start();
        this.client.getConnectionStateListenable().addListener(this);
        this.lock = new InterProcessMutex(client, basePath + LOCK_PATH);
        try {
            instancePath = this.client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(basePath + INSTANCES_PATH);
            LOGGER.debug("instancePath:{}", instancePath);
        } catch (Exception e) {
            instancePath = basePath + INSTANCES_PATH;
        }
        this.tryToGetId();
    }

    @Override
    public synchronized void stateChanged(CuratorFramework client, ConnectionState state) {
        try {
            if (ConnectionState.LOST.equals(state) || ConnectionState.SUSPENDED.equals(state)) {
                LOGGER.info("zk connection state :{} , prepare to releaseId ...", state);
                releaseId();
                client.getZookeeperClient().blockUntilConnectedOrTimedOut();
                LOGGER.info("zk connection is reconnected , prepare to tryToGetId ...");
                int id = tryToGetId();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int getMaxNumberOfInstances() {
        return this.maxNumberOfInstances;
    }

    @Override
    public synchronized int tryToGetId() throws Exception {
        long startTime = System.currentTimeMillis();
        if (currentInstancePath != null && currentId != -1) {
            LOGGER.info("not need to reacquare，return current id {} directly", this.currentId);
            return this.currentId;
        }
        LOGGER.info("to get new id ...");
        if (CuratorFrameworkState.LATENT.equals(this.client.getState())) {
            this.client.start();
        }
        lock.acquire();
        int allocatedId = 0;
        try {
            List<String> children = client.getChildren().forPath(instancePath);
            if (children.size() == 0) {
                allocatedId = 0;
            } else {
                int[] ids = new int[children.size()];
                int counter = 0;
                for (String child : children) {
                    ids[counter++] = getLeafValueForNode(child);
                }
                Arrays.sort(ids);
                for (int id : ids) {
                    if (id != allocatedId) {
                        allocatedId = id;
                        LOGGER.debug("currentInstancePath:{}", currentInstancePath);
                        break;
                    }
                    allocatedId++;
                }
            }
            if (allocatedId > getMaxNumberOfInstances()) {
                throw new RuntimeException(String.format("instance id over flow , current id : %s , max id : %s", allocatedId, getMaxNumberOfInstances()));
            }
            this.currentId = allocatedId;
            currentInstancePath = this.client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(instancePath + "/" + INSTANCE_PATH + allocatedId, InetAddress.getLocalHost().toString().getBytes());
            LOGGER.debug("currentInstancePath:{}", currentInstancePath);
            LOGGER.info("get new id ：{}", this.currentId);
            return this.currentId;
        } finally {
            lock.release();
            long endTime = System.currentTimeMillis();
            LOGGER.info("tryToGetId cost {} ms", this.currentId, (endTime - startTime));
        }
    }

    @Override
    public synchronized int getCurrentId() {
        return currentId;
    }

    @Override
    public synchronized InstanceIdManager releaseId() {
        if (this.currentInstancePath != null) {
            try {
                this.client.delete().deletingChildrenIfNeeded().forPath(this.currentInstancePath);
            } catch (Exception e) {
                if (!(e instanceof KeeperException.NoNodeException)) {
                    throw new RuntimeException(e);
                } else {
                    LOGGER.debug("delete current znode  {}  fail : znode not exist , ignore ...", currentInstancePath);
                }
            }
        }
        this.currentId = -1;
        this.currentInstancePath = null;
        return this;
    }

    @Override
    public void close() {
        this.client.close();
    }

    private int getLeafValueForNode(String child) {
        int lastIndex = child.lastIndexOf("/");
        String lastPath = lastIndex >= 0 ? child.substring(lastIndex + 1) : child;
        return Integer.parseInt(lastPath.replace(INSTANCE_PATH, ""));
    }

}
