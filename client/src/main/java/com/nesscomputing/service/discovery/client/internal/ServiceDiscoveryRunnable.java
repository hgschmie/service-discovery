/**
 * Copyright (C) 2012 Ness Computing, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nesscomputing.service.discovery.client.internal;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import com.nesscomputing.logging.Log;
import com.nesscomputing.service.discovery.client.DiscoveryClientConfig;
import com.nesscomputing.service.discovery.job.ZookeeperProcessingTask;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * Main processing code for service discovery. Ensures that there is a regular scan of
 * the directory even if no state changes occur, runs all the service discovery tasks accordingly.
 *
 * The read only client only runs the service discovery reader, the read/write client runs the reader and
 * the announcer.
 */
class ServiceDiscoveryRunnable extends ZookeeperProcessingTask
{
    private static final Log LOG = Log.findLog();

    private final String discoveryRoot;
    private final long scanTicks;

    private volatile long lastScan = 0;

    private final Set<ServiceDiscoveryTask> visitors;

    ServiceDiscoveryRunnable(final String connectString,
                             final DiscoveryClientConfig discoveryConfig,
                             @Nonnull final Set<ServiceDiscoveryTask> visitors)
    {
        super(connectString, discoveryConfig.getTickInterval().getMillis());

        this.visitors = visitors;

        this.scanTicks = discoveryConfig.getScanInterval().getMillis() / discoveryConfig.getTickInterval().getMillis();
        this.discoveryRoot = discoveryConfig.getRoot();
        LOG.info("Scan Ticks is %d (Tick interval is %dms)", scanTicks, discoveryConfig.getTickInterval().getMillis());
    }

    /**
     * Trigger the loop every time enough ticks have been accumulated, or whenever any of the
     * visitors requests it.
     */
    @Override
    public long determineCurrentGeneration(final AtomicLong generation, final long tick)
    {
        // If the scan interval was reached, trigger the
        // run.
        if (tick - lastScan >= scanTicks) {
            lastScan = tick;
            return generation.incrementAndGet();
        }

        // Otherwise, give the service discovery serviceDiscoveryVisitors a chance
        // to increment the generation.
        for (ServiceDiscoveryTask visitor : visitors) {
            visitor.determineGeneration(generation, tick);
        }

        return generation.get();
    }

    @Override
    protected boolean doWork(final ZooKeeper zookeeper, final long tick) throws KeeperException, IOException, InterruptedException
    {
        LOG.debug("Scanning (tick is %d)...", tick);
        // Hook up with the current Runnable as watcher. Whenever a child event happens, this will
        // trigger a new scan of the children.
        final List<String> childNodes = zookeeper.getChildren(discoveryRoot, this);

        for (ServiceDiscoveryTask visitor : visitors) {
            visitor.visit(childNodes, zookeeper, tick);
        }

        return true;
    }
}
