package ink.andromeda.strawberry.core;

import ink.andromeda.strawberry.tools.Pair;

import java.util.List;
import java.util.Map;

/**
 * 数据集内部原始表之间的关系
 *
 */
public class VirtualRelation {

    private Map<String, LinkNode> linkNodeMap;





    public static class LinkNode {

        private String tableName;

        private Map<String, List<Pair<String, String>>> prev;

        private Map<String, List<Pair<String, String>>> next;

    }

}
