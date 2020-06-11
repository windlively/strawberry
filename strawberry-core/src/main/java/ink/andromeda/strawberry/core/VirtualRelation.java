package ink.andromeda.strawberry.core;

import ink.andromeda.strawberry.tools.Pair;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ink.andromeda.strawberry.tools.GeneralTools.toJSONString;

/**
 * 原始表之间的关系
 *
 */
public class VirtualRelation {

    @Setter
    @Getter
    private Map<String, VirtualNode> virtualNodeMap;

    @Setter
    @Getter
    private Map<String, String> tableLabelRef;

    @Setter
    @Getter
    private Map<String, List<String>> whereCases;


    @Getter
    @Accessors(fluent = true)
    @ToString
    public static class VirtualNode {

        private final String tableName;

        public VirtualNode(String tableName){
            this.tableName = tableName;
        }

        private final Map<String, JoinProfile> prev = new HashMap<>();

        private final Map<String, JoinProfile> next = new HashMap<>();

    }

    @Override
    public String toString() {
        return toJSONString(this);
    }
}
