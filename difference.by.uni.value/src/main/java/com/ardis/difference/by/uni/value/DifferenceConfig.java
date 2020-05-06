package com.ardis.difference.by.uni.value;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "ardis.difference.by.uni.value")
public class DifferenceConfig {
    private String collection = "node_organization";
    private String databaseNew = "entity_jo_registry";
    private String databaseOld = "entity_jo_registry_old";
    private String databaseDif = "result";
 /* private String databaseNew;
    private String collection;
    private String databaseOld;
    private String databaseDif;
    */

}
