package ink.andromeda.strawberry.core;

import ink.andromeda.strawberry.tools.Pair;

import java.util.List;
import java.util.Map;

/**
 * 数据集内部原始表之间的关系
 *
 */
public class VirtualRelation {







    public static class LinkReference {

        private String tableName;

        private Map<String, List<Pair<String, String>>> prev;

        private List<String> next;

    }

}
