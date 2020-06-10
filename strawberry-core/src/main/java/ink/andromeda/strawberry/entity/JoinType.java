package ink.andromeda.strawberry.entity;

import java.util.Objects;

/**
 * 连接类型
 */
public enum JoinType {
    LEFT_JOIN,
    JOIN,
    RIGHT_JOIN,
    FULL_JOIN;

    public static JoinType of(String str){
        Objects.requireNonNull(str);
        str = str.trim();
        if(str.matches("(?i)(JOIN)")) {
            return JOIN;
        }
        if(str.matches("(?i)(LEFT\\s+JOIN)")) {
            return LEFT_JOIN;
        }
        if(str.matches("(?i)(RIGHT\\s+JOIN)")) {
            return RIGHT_JOIN;
        }
        if(str.matches("(?i)(FULL\\s+JOIN)")) {
            return FULL_JOIN;
        }
        throw new IllegalArgumentException("unrecognized join type: " + str);
    }
}
