package net.explorviz.reconstructor.stream;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.quarkus.arc.DefaultBean;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class SchemaRegistryClientFactory {

  @ConfigProperty(name = "explorviz.schema-registry.url")
  String schemaRegistryUrl;

  @Produces
  @DefaultBean
  public SchemaRegistryClient schemaRegistryClient() {
    return new CachedSchemaRegistryClient("http://" + this.schemaRegistryUrl, 10);
  }
}
