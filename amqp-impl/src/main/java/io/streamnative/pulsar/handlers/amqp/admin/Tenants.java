package io.streamnative.pulsar.handlers.amqp.admin;

import io.streamnative.pulsar.handlers.amqp.admin.impl.BaseResources;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/tenants")
@Produces(MediaType.APPLICATION_JSON)
public class Tenants extends BaseResources {

    @GET
    public void getList(@Suspended final AsyncResponse response) {
        getAllTenantListAsync()
                .thenAccept(response::resume)
                .exceptionally(t -> {
                    log.error("Failed to get vhost list for tenant {}", tenant, t);
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }

    @GET
    @Path("/loadAllExchange")
    public void loadAllExchange(@Suspended final AsyncResponse response) {
        getAllTenantListAsync()
                .thenAccept(names -> names.forEach(name -> amqpAdmin().loadAllVhostForExchange(name.getName())))
                .thenAccept(__ -> response.resume(Response.noContent().build()))
                .exceptionally(t -> {
                    resumeAsyncResponseExceptionally(response, t);
                    return null;
                });
    }
}
