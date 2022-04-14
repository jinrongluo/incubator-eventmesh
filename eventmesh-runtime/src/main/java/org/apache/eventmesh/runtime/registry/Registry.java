/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.eventmesh.runtime.registry;

import org.apache.eventmesh.api.registry.RegistryService;
import org.apache.eventmesh.api.registry.SubscriberService;
import org.apache.eventmesh.api.registry.dto.EventMeshDataInfo;
import org.apache.eventmesh.api.registry.dto.EventMeshRegisterInfo;
import org.apache.eventmesh.api.registry.dto.EventMeshUnRegisterInfo;
import org.apache.eventmesh.api.registry.dto.RegistryEvent;
import org.apache.eventmesh.api.registry.dto.SubscriberInfo;
import org.apache.eventmesh.spi.EventMeshExtensionFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Registry {
    private static final Logger logger = LoggerFactory.getLogger(Registry.class);
    private static RegistryService registryService;

    private static SubscriberService subscriberService;

    public void init(String registryPluginType) throws Exception {
        registryService = EventMeshExtensionFactory.getExtension(RegistryService.class, registryPluginType);
        if (registryService == null) {
            logger.error("can't load the registryService plugin, please check.");
            throw new RuntimeException("doesn't load the registryService plugin, please check.");
        }
        registryService.init();

        subscriberService = EventMeshExtensionFactory.getExtension(SubscriberService.class, registryPluginType);
        if (subscriberService == null) {
            logger.error("can't load the subscriber registryService plugin, please check.");
            throw new RuntimeException("doesn't load the subscriber registryService plugin, please check.");
        }
        subscriberService.init();
    }

    public void start() throws Exception {
        registryService.start();
        subscriberService.start();
    }

    public void shutdown() throws Exception {
        registryService.shutdown();
        subscriberService.shutdown();
    }

    public List<EventMeshDataInfo> findEventMeshInfoByCluster(String clusterName) throws Exception {
        return registryService.findEventMeshInfoByCluster(clusterName);
    }

    public Map<String, Map<String, Integer>> findEventMeshClientDistributionData(String clusterName, String group, String purpose) throws Exception {
        return registryService.findEventMeshClientDistributionData(clusterName, group, purpose);
    }

    public boolean register(EventMeshRegisterInfo eventMeshRegisterInfo) throws Exception {
        return registryService.register(eventMeshRegisterInfo);
    }

    public boolean unRegister(EventMeshUnRegisterInfo eventMeshUnRegisterInfo) throws Exception {
        return registryService.unRegister(eventMeshUnRegisterInfo);
    }

    public boolean registerSubscriber(SubscriberInfo subscriberInfo) throws Exception {
        return subscriberService.register(subscriberInfo);
    }

    public boolean unRegisterSubscriber(SubscriberInfo subscriberInfo) throws Exception {
        return subscriberService.unRegister(subscriberInfo);
    }

    public List<SubscriberInfo> listSubscribers() throws Exception {
        return subscriberService.list();
    }

    public boolean updateSubscriber(SubscriberInfo subscriberInfo) throws Exception {
        return subscriberService.update(subscriberInfo);
    }

    public SubscriberInfo getSubscriber(String id) throws Exception {
        return subscriberService.get(id);
    }

    public boolean watch(Consumer<RegistryEvent<SubscriberInfo>> onEvent, Consumer<Throwable> onError) throws Exception {
        return subscriberService.watch(onEvent, onError);
    }
}
