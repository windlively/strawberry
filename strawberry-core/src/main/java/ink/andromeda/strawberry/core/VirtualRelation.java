//package ink.andromeda.strawberry.core;
//
//import lombok.Getter;
//import org.springframework.util.Assert;
//
//import java.util.*;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
///**
// * 数据集内部原始表之间的关系
// * <p>包含左右连接(join)和上下连接(union)
// */
//public class VirtualRelation {
//
//    private final LinkedList<JoinConfig> joinConfigChain = new LinkedList<>();
//
//    private JoinConfig first;
//
//    private JoinConfig last;
//
//    /**
//     * 根据左连接点查找连接配置的索引表
//     */
//    @Getter
//    private Map<String, JoinConfig> leftPointIndexMap;
//
//    /**
//     * 根据右连接点查找连接配置的索引表
//     */
//    @Getter
//    private Map<String, JoinConfig> rightPointIndexMap;
//
//    public VirtualDataSet.VirtualTable initNewVirtualTable() {
//        VirtualDataSet.VirtualTable virtualTable = new VirtualDataSet.VirtualTable();
//        List<VirtualDataSet.OriginalResultSet> originalResultSetList = new ArrayList<>();
//        VirtualDataSet.OriginalResultSet head = new VirtualDataSet.OriginalResultSet(joinConfigChain.getFirst().leftTable);
//        originalResultSetList.add(head);
//        joinConfigChain.stream().skip(0).forEach(jc -> originalResultSetList.add(new VirtualDataSet.OriginalResultSet(jc.getRightTable())));
//        virtualTable.setSourceResultSetList(originalResultSetList.stream().collect(Collectors.toMap(VirtualDataSet.OriginalResultSet::getName, s -> s)));
//        return virtualTable;
//    }
//
//    public VirtualRelation(JoinConfig... configs) {
//        processJoinRelation(configs);
//    }
//
//    /**
//     * 将连接关系按照顺序组合, 同时检查所给的连接关系是否正常
//     *
//     * @param configs 连接配置
//     */
//    private void processJoinRelation(JoinConfig... configs) {
//        Assert.notEmpty(configs, "join config array could be not null!");
//
//        List<JoinConfig> candidateList = Arrays.asList(configs);
//        // 参数检查
//        candidateList.forEach(c -> {
//            if (c.getJoinFields() == null || c.getJoinFields().isEmpty()) {
//                throw new IllegalArgumentException(String.format("table %s and %s join fields is empty!", c.getRightTable(), c.getLeftTable()));
//            }
//        });
//
//        int count = candidateList.size();
//        JoinConfig first = candidateList.get(0);
//        String currentLeft = first.leftTable;
//        String currentRight = first.rightTable;
//        joinConfigChain.add(first);
//        count--;
//        while (count > 0) {
//            String finalCurrentRight = currentRight;
//            List<JoinConfig> filter = candidateList.stream().filter(c -> Objects.equals(finalCurrentRight, c.leftTable)).collect(Collectors.toList());
//
//            if (filter.size() > 1) {
//                // 查找到多个左连接点
//                throw new IllegalStateException(String.format("given join config '%s' has multi left join point '%s'!", toJSONString(configs), currentRight));
//            }
//            if (filter.size() == 0)
//                break;
//
//            JoinConfig next = filter.get(0);
//            joinConfigChain.add(next);
//            currentRight = next.rightTable;
//            count--;
//        }
//
//        while (count > 0) {
//            String finalCurrentLeft = currentLeft;
//            List<JoinConfig> filter = candidateList.stream().filter(c -> Objects.equals(finalCurrentLeft, c.rightTable)).collect(Collectors.toList());
//            if (filter.size() > 1) {
//                // 查找到多个右连接点
//                throw new IllegalStateException(String.format("given join config '%s' has multi right join point '%s'!", toJSONString(configs), currentLeft));
//            }
//            if (filter.size() == 0)
//                break;
//
//            JoinConfig prev = filter.get(0);
//            joinConfigChain.addFirst(prev);
//            currentLeft = prev.leftTable;
//            count--;
//        }
//
//        if (count != 0) {
//            throw new IllegalStateException(String.format("given join config '%s' is not discontinuously! join result: '%s'", toJSONString(configs),
//                    toJSONString(joinConfigChain.stream().map(j -> j.getLeftTable() + "--" + j.getRightTable()).collect(Collectors.joining(",")))));
//        }
//        this.first = joinConfigChain.getFirst();
//        this.last = joinConfigChain.getLast();
//        leftPointIndexMap = Collections.unmodifiableMap(joinConfigChain.stream().collect(Collectors.toMap(j -> j.leftTable, j -> j)));
//        rightPointIndexMap = Collections.unmodifiableMap(joinConfigChain.stream().collect(Collectors.toMap(j -> j.rightTable, j -> j)));
//    }
//
//    /**
//     * @param current 当前表名
//     * @return 前一个表的名称
//     */
//    public String getPrevTableName(String current) {
//        if (Objects.equals(current, first.getLeftTable()))
//            return null;
//        return Objects.requireNonNull(rightPointIndexMap.get(current)).leftTable;
//    }
//
//    /**
//     * @param current 当前表名
//     * @return 后一个表的名称
//     */
//    public String getNextTableName(String current) {
//        if (Objects.equals(current, last.getRightTable()))
//            return null;
//        return Objects.requireNonNull(leftPointIndexMap.get(current)).rightTable;
//    }
//
//    /**
//     * @param current 当前表名
//     * @return 向前(左)连接时的连接字段
//     */
//    public String[] getPrevJoinFields(String current) {
//        if (Objects.equals(current, first.getLeftTable()))
//            return new String[0];
//        return Objects.requireNonNull(rightPointIndexMap.get(current)).getJoinFields().stream().map(c -> c.getRight().fullFieldName()).toArray(String[]::new);
//    }
//
//    /**
//     * @param current 当前表名
//     * @return 向后(右)连接时的字段
//     */
//    public String[] getNextJoinFields(String current) {
//        if (Objects.equals(current, last.getRightTable()))
//            return new String[0];
//        return Objects.requireNonNull(leftPointIndexMap.get(current)).getJoinFields().stream().map(c -> c.getLeft().fullFieldName()).toArray(String[]::new);
//    }
//
//    // 表之间的联合关系
//    private final Map<String, List<VirtualDataSet.UnionConfig>> unionConfigMapping = new HashMap<>();
//
//    private final Map<String, VirtualDataSet.UnionConfig> unionConfigCache = new HashMap<>();
//
//    public void addUnionConfigs(VirtualDataSet.UnionConfig... configs) {
//        processUnionRelation(configs);
//    }
//
//    private void processUnionRelation(VirtualDataSet.UnionConfig... configs) {
//        Assert.notEmpty(configs, "union config array could be not null!");
//
//        unionConfigMapping.putAll(
//                Stream.of(configs)
//                        .collect(Collectors.groupingBy(
//                                s -> s.getMainTable().fullName()))
//        );
//    }
//
//    public List<VirtualDataSet.UnionConfig> getUnionConfigs(String mainTableFullName) {
//        return unionConfigMapping.get(mainTableFullName);
//    }
//
//    public VirtualDataSet.UnionConfig getUnionConfig(String mainTableFullName, String unionTableFullName) {
//        return unionConfigCache.computeIfAbsent(mainTableFullName + "|" + unionTableFullName, k ->
//                Objects.requireNonNull(unionConfigMapping.get(mainTableFullName))
//                        .stream()
//                        .filter(s -> Objects.equals(s.getUnionTable().fullName(), unionTableFullName))
//                        .findFirst()
//                        .orElseThrow(NullPointerException::new)
//        );
//    }
//
//    @Override
//    public String toString() {
//        return first.leftTable() + joinConfigChain.stream()
//                .map(c -> " <==" + Optional.ofNullable(c.getJoinFields()).orElse(new HashSet<>()).stream().map(p -> p.getLeft().fullFieldName() + "<->" + p.getRight().fullFieldName())
//                        .collect(Collectors.joining(", ", "[", "]")) + "==> " + c.getRightTable()).reduce(String::concat).orElse(null);
//    }
//
//}
