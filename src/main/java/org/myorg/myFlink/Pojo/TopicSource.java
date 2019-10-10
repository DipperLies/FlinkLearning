package org.myorg.myFlink.Pojo;

import lombok.Data;

/**
 * @author Michael
 * @date 2019-10-10 9:36
 */
@Data
public class TopicSource {
    private long time;
    private String id;
    private String old_state;
    private String new_state;
    private String eqp_id;
}
