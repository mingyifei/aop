/**
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
package io.streamnative.pulsar.handlers.amqp;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.streamnative.pulsar.handlers.amqp.admin.AmqpAdmin;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.namespace.NamespaceBundleOwnershipListener;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.util.FutureUtil;

/**
 * Connection container, listen bundle unload event, release connection resource.
 */
@Slf4j
public class ConnectionContainer {

    @Getter
    private final Map<NamespaceName, Set<AmqpConnection>> connectionMap = Maps.newConcurrentMap();
    private AmqpAdmin amqpAdmin;

    protected ConnectionContainer(PulsarService pulsarService,
                                  ExchangeContainer exchangeContainer, QueueContainer queueContainer,
                                  AmqpAdmin amqpAdmin) {
        this.amqpAdmin = amqpAdmin;
        pulsarService.getNamespaceService().addNamespaceBundleOwnershipListener(new NamespaceBundleOwnershipListener() {
            @Override
            public void onLoad(NamespaceBundle namespaceBundle) {
                int brokerPort = pulsarService.getBrokerListenPort().isPresent()
                        ? pulsarService.getBrokerListenPort().get() : 0;
                log.info("ConnectionContainer [onLoad] namespaceBundle: {}, brokerPort: {}",
                        namespaceBundle, brokerPort);
            }

            @Override
            public void unLoad(NamespaceBundle namespaceBundle) {
                log.info("ConnectionContainer [unLoad] namespaceBundle: {}", namespaceBundle);
                NamespaceName namespaceName = namespaceBundle.getNamespaceObject();
                exchangeContainer.getExchangeMap().remove(namespaceName);
                queueContainer.getQueueMap().remove(namespaceName);
                /*Map<String, CompletableFuture<AmqpExchange>> exchangeMap =
                        exchangeContainer.getExchangeMap().remove(namespaceName);
                if (exchangeMap != null) {
                    List<CompletableFuture<Void>> futures = exchangeMap.values().stream()
                            .map(future -> future.thenCompose(amqpExchange -> {
                                amqpExchange.close();
                                return amqpAdmin.loadExchange(namespaceName, amqpExchange.getName());
                            }))
                            .collect(Collectors.toList());
                    FutureUtil.waitForAll(futures);
                    exchangeMap.clear();
                }

                Map<String, CompletableFuture<AmqpQueue>> queueMap =
                        queueContainer.getQueueMap().remove(namespaceName);
                if (queueMap != null) {
                    List<CompletableFuture<Void>> futures = queueMap.values().stream()
                            .map(future -> future.thenCompose(amqpQueue -> {
                                amqpQueue.close();
                                return amqpAdmin.loadQueue(namespaceName, amqpQueue.getName());
                            }))
                            .collect(Collectors.toList());
                    FutureUtil.waitForAll(futures);
                    queueMap.clear();
                }*/
            }

            @Override
            public boolean test(NamespaceBundle namespaceBundle) {
                return true;
            }
        });
    }

    public void addConnection(NamespaceName namespaceName, AmqpConnection amqpConnection) {
        connectionMap.compute(namespaceName, (ns, connectionSet) -> {
            if (connectionSet == null) {
                connectionSet = Sets.newConcurrentHashSet();
            }
            connectionSet.add(amqpConnection);
            return connectionSet;
        });
    }

    public void removeConnection(NamespaceName namespaceName, AmqpConnection amqpConnection) {
        if (namespaceName == null) {
            return;
        }
        connectionMap.getOrDefault(namespaceName, Collections.emptySet()).remove(amqpConnection);
    }

}
