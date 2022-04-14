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

package org.apache.eventmesh.api.registry;

import org.apache.eventmesh.api.exception.RegistryException;
import org.apache.eventmesh.api.registry.dto.EventMeshDataInfo;
import org.apache.eventmesh.api.registry.dto.EventMeshRegisterInfo;
import org.apache.eventmesh.api.registry.dto.EventMeshUnRegisterInfo;
import org.apache.eventmesh.api.registry.dto.RegistryEvent;
import org.apache.eventmesh.api.registry.dto.SubscriberInfo;
import org.apache.eventmesh.spi.EventMeshExtensionType;
import org.apache.eventmesh.spi.EventMeshSPI;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * RegistryService
 */
@EventMeshSPI(isSingleton = true, eventMeshExtensionType = EventMeshExtensionType.REGISTRY)
public interface SubscriberService {
    void init() throws RegistryException;

    void start() throws RegistryException;

    void shutdown() throws RegistryException;

    boolean register(SubscriberInfo subscriberInfo) throws RegistryException;

    boolean unRegister(SubscriberInfo subscriberInfo) throws RegistryException;

    SubscriberInfo get(String id) throws RegistryException;

    List<SubscriberInfo> list() throws RegistryException;

    boolean update(SubscriberInfo subscriberInfo) throws RegistryException;

    boolean watch(Consumer<RegistryEvent<SubscriberInfo>> onEvent, Consumer<Throwable> onError);
}
