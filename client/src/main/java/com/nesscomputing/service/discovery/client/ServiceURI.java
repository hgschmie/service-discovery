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
package com.nesscomputing.service.discovery.client;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Represents a srvc:// URI. The canonical format is srvc://serviceName[:serviceType]/path?query#fragment
 */
public final class ServiceURI
{
    private final String serviceName;
    private final String serviceType;
    private final String path;
    private final String query;
    private final String fragment;

    public static ServiceURI valueOf(final String uri)
    {
        try {
            return new ServiceURI(uri);
        }
        catch (final URISyntaxException use) {
            throw new IllegalArgumentException(use);
        }
    }

    public ServiceURI(final String uri) throws URISyntaxException
    {
        this(new URI(uri));
    }

    public ServiceURI(final URI uri) throws URISyntaxException
    {
        if (!"srvc".equals(uri.getScheme())) {
            throw new URISyntaxException(uri.toString(), "ServiceURI only supports srvc:// URIs");
        }
        if (!StringUtils.startsWith(uri.getSchemeSpecificPart(), "//")) {
            throw new URISyntaxException(uri.toString(), "ServiceURI only supports srvc:// URIs");
        }

        final String schemeSpecificPart = uri.getSchemeSpecificPart().substring(2);
        final int slashIndex = schemeSpecificPart.indexOf('/');
        if (slashIndex == -1) {
            throw new URISyntaxException(uri.toString(), "ServiceURI requires a slash at the end of the service!");
        }
        final int colonIndex = schemeSpecificPart.indexOf(':');
        if (colonIndex == -1 || colonIndex > slashIndex) {
            serviceName = schemeSpecificPart.substring(0, slashIndex);
            serviceType = null;
        }
        else {
            serviceName = schemeSpecificPart.substring(0, colonIndex);
            serviceType = schemeSpecificPart.substring(colonIndex + 1, slashIndex);
        }

        path = uri.getRawPath();
        query = uri.getRawQuery();
        fragment = uri.getRawFragment();
    }

    public String getServiceName()
    {
        return serviceName;
    }

    public String getServiceType()
    {
        return serviceType;
    }

    public String getPath()
    {
        return path;
    }

    public String getQuery()
    {
        return query;
    }

    public String getFragment()
    {
        return fragment;
    }

    @Override
    public boolean equals(final Object other)
    {
        if (!(other instanceof ServiceURI)) {
            return false;
        }
        final ServiceURI castOther = (ServiceURI) other;
        return new EqualsBuilder().append(serviceName, castOther.serviceName).append(serviceType, castOther.serviceType).append(path, castOther.path).append(query, castOther.query).append(fragment, castOther.fragment).isEquals();
    }

    private transient int hashCode;

    @Override
    public int hashCode()
    {
        if (hashCode == 0) {
            hashCode = new HashCodeBuilder().append(serviceName).append(serviceType).append(path).append(query).append(fragment).toHashCode();
        }
        return hashCode;
    }

    private transient String toString;

    @Override
    public String toString()
    {
        if (toString == null) {
            toString = new ToStringBuilder(this).append("serviceName", serviceName).append("serviceType", serviceType).append("path", path).append("query", query).append("fragment", fragment).toString();
        }
        return toString;
    }
}
