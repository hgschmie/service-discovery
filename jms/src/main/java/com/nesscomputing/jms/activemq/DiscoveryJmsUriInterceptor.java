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
package com.nesscomputing.jms.activemq;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import com.nesscomputing.jms.JmsUriInterceptor;
import com.nesscomputing.logging.Log;
import com.nesscomputing.service.discovery.client.ReadOnlyDiscoveryClient;

import org.apache.commons.lang3.StringUtils;

/**
 * Replace the single format specifier in a srvc:// URI with the unique ID specifying which
 * injector it belongs to.
 */
@Singleton
class DiscoveryJmsUriInterceptor implements JmsUriInterceptor {
    private static final Log LOG = Log.findLog();
    private final UUID injectorId = UUID.randomUUID();
    private final DiscoveryJmsConfig config;

    @Inject
    DiscoveryJmsUriInterceptor(ReadOnlyDiscoveryClient discoveryClient, DiscoveryJmsConfig config)
    {
        this.config = config;

        LOG.debug("Waiting for world change then registering discovery client %s", injectorId);
        // Ensure that we don't register a discovery client until it's had at least one world-change (or give up due to timeout)
        try {
            discoveryClient.waitForWorldChange(config.getDiscoveryTimeout().getMillis(), TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        ServiceDiscoveryTransportFactory.registerDiscoveryClient(injectorId, discoveryClient, config);
    }

    @Override
    public String transform(String connectionName, String input)
    {
        Preconditions.checkState(config != null, "no config for %s", injectorId);
        Preconditions.checkState(config.isSrvcTransportEnabled(), "srvc not enabled, the module should not have bound this interceptor");

        if (!config.isSrvcTransportEnabled() || StringUtils.isEmpty(input) || !input.startsWith("srvc://"))
        {
            return input;
        }

        return String.format(input, injectorId);
    }
}
