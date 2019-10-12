package org.myorg.myFlink.Pojo;

import lombok.Data;

import java.io.Serializable;

/**
 * @author Michael
 * @date 2019-10-10 9:36
 */
@Data
public class TopicSource implements Serializable {
    private String time;
    private String id;
    private String old_state;
    private String new_state;
    private String eqp_id;

    public TopicSource(){
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TopicSource topicSource = (TopicSource) o;
        return  (time != null ? time.equals(topicSource.time) : topicSource.time == null)
                && (id != null ? id.equals(topicSource.id) : topicSource.id == null)
                && (old_state != null ? old_state.equals(topicSource.old_state) : topicSource.old_state == null)
                && (new_state != null ? new_state.equals(topicSource.new_state) : topicSource.new_state == null)
                && (eqp_id != null ? eqp_id.equals(topicSource.eqp_id) : topicSource.eqp_id == null)
                ;
    }
}
