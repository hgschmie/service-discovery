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
package com.nesscomputing.service.discovery.server.job;

import java.io.IOException;

import com.nesscomputing.logging.Log;
import com.nesscomputing.service.discovery.job.ZookeeperJob;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * Creates a given path of nodes in zookeeper. The nodes are created persistent, so they will be around until
 * explicitly deleted.
 */
public class BuildPathJob extends ZookeeperJob
{
    private static final Log LOG = Log.findLog();

    private final String [] elements;

    public BuildPathJob(final String path)
    {
        this.elements = StringUtils.split(path, "/");
    }

    @Override
    protected boolean execute(final ZooKeeper zookeeper) throws KeeperException, IOException
    {
        if (elements.length > 0) {
            final StringBuilder sb = new StringBuilder("");

            for (String element : elements) {
                if (StringUtils.isBlank(element)) {
                    continue;
                }

                sb.append('/').append(element);

                final String zPath = sb.toString();
                try {
                    if (zookeeper.exists(zPath, false) == null) {
                        LOG.info("Node %s does not exist, creating", zPath);
                        zookeeper.create(zPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    }
                }
                catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return true;
                }
                catch (KeeperException ke) {
                    if (ke.code() == Code.NODEEXISTS) {
                        LOG.trace("Node exists, ignoring");
                    }
                    else {
                        throw ke;
                    }
                }
            }
        }
        return true;
    }
}
