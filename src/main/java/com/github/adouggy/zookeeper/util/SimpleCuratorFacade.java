package com.github.adouggy.zookeeper.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * try zookeeper
 * @author liyazi
 * @date 2015年12月1日
 */
public enum SimpleCuratorFacade {
    INSTANCE;

    private CuratorFramework client = null;
    private static final String connStr = "localhost:2181,localhost:2182,localhost:2183";

    private SimpleCuratorFacade() {
        client = createFramework();
    }

    private CuratorFramework createFramework() {
        RetryPolicy policy = new ExponentialBackoffRetry(1000, 3);
        Builder builder = CuratorFrameworkFactory.builder().connectString(connStr)
                .connectionTimeoutMs(5000).sessionTimeoutMs(5000).retryPolicy(policy)
                .namespace("zkUtil");
        CuratorFramework client = builder.build();
        client.start();
        return client;
    }

    public InterProcessMutex getLock(String path) {
        return new InterProcessMutex(client, path);
    }

    public String getString(String path) {
        try {
            byte[] bytes = client.getData().forPath("/test");
            if (bytes != null) {
                return new String(bytes);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        System.out.println(SimpleCuratorFacade.INSTANCE.getString("/test"));

        final InterProcessMutex lock = SimpleCuratorFacade.INSTANCE.getLock("/test_lock");
        Thread t1 = new Thread() {

            @Override
            public void run() {
                try {
                    lock.acquire();

                    for (int i = 0; i < 10; i++) {
                        System.out.println("t1:" + i);
                        sleep(100);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        lock.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        };

        Thread t2 = new Thread() {

            @Override
            public void run() {
                try {
                    lock.acquire();

                    for (int i = 0; i < 10; i++) {
                        System.out.println("t2:" + i);
                        sleep(100);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        lock.release();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
        };

        t1.start();
        t2.start();
    }
}
