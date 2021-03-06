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
package com.nesscomputing.service.discovery.server.zookeeper;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import com.nesscomputing.logging.Log;

import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.Util;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Periodic job cleaning out the data and log directories. Taken from org.apache.zookeeper.server.PurgeTxnLog and then sanitized.
 */
public class ZookeeperCleanupJob implements Job
{
    private static final Log LOG = Log.findLog();

    private static final int KEEP_VERSIONS_COUNT = 10;
    private final FileTxnSnapLog fileTxnSnapLog;

    @Inject
    public ZookeeperCleanupJob(final FileTxnSnapLog fileTxnSnapLog)
    {
        this.fileTxnSnapLog = fileTxnSnapLog;
    }

    @Override
    public void execute(final JobExecutionContext context) throws JobExecutionException
    {
        try {
            LOG.info("Running zookeeper cleanup.");

            // found any valid recent snapshots?
            // files to exclude from deletion
            final Set<File> excludes = Sets.newHashSet();
            final List<File> snaps = fileTxnSnapLog.findNRecentSnapshots(KEEP_VERSIONS_COUNT);

            if (!snaps.isEmpty())  {
                File snapFile = null;

                for(Iterator<File> it = snaps.iterator(); it.hasNext(); ) {
                    snapFile = it.next();
                    excludes.add(snapFile);
                }

                // add the snapshots for the last file in the list.
                final long zxid = Util.getZxidFromName(snapFile.getName(), "snapshot");
                excludes.addAll(Arrays.asList(fileTxnSnapLog.getSnapshotLogs(zxid)));

                final List<File> files = Lists.newArrayList();

                // add all non-excluded log files
                files.addAll(Arrays.asList(fileTxnSnapLog.getDataDir().listFiles(new PrefixExcludesFilter("log.", excludes))));

                // add all non-excluded snapshot files to the deletion list
                files.addAll(Arrays.asList(fileTxnSnapLog.getSnapDir().listFiles(new PrefixExcludesFilter("snapshot.", excludes))));

                // remove the old files
                if (!files.isEmpty()) {
                    for (File file: files) {
                        LOG.info("Removing file %s (%s)", file.getPath(), DateFormat.getDateTimeInstance().format(file.lastModified()));

                        if(!file.delete()) {
                            LOG.warn("Failed to remove %s", file.getPath());
                        }
                    }
                }
                else {
                    LOG.info("No files to remove.");
                }
            }
        }
        catch (IOException ioe) {
            throw new JobExecutionException(ioe);
        }
    }

    private static class PrefixExcludesFilter implements FileFilter
    {
        private final String prefix;
        private final Set<File> excludes;

        PrefixExcludesFilter(final String prefix, final Set<File> excludes)
        {
            this.prefix = prefix;
            this.excludes = excludes;
        }

        @Override
        public boolean accept(final File f)
        {
            return f.getName().startsWith(prefix) &&  !excludes.contains(f);
        }
    }
}
