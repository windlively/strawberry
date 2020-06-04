package ink.andromeda.strawberry.core;

import ink.andromeda.strawberry.tools.Pair;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据集内部原始表之间的关系
 *
 */
public class VirtualRelation {

    @Setter
    private Map<String, VirtualNode> virtualNodeMap;

    @Setter
    private Map<String, String> tableLabelRef;



    @Getter
    @Accessors(fluent = true)
    @ToString
    public static class VirtualNode {

        private final String tableName;

        public VirtualNode(String tableName){
            this.tableName = tableName;
        }

        private final Map<String, List<Pair<String, String>>> prev = new HashMap<>();

        private final Map<String, List<Pair<String, String>>> next = new HashMap<>();

    }

}
