package org.poem.config.table;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @author poem
 */
@Data
@Component
@ConfigurationProperties(prefix = "spring.trasform")
public class TransformTables {

    /**
     *
     */
    private String tables;
}
