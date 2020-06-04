package ink.andromeda.strawberry.tools;

import java.util.Map;

public interface Pair<L, R> extends Map.Entry<L, R>{

    static <L, R> Pair<L, R> of(L left, R right){
        return new ImmutablePair<>(left, right);
    }

    L getLeft();

    R getRight();

    void setLeft(L left);

    void setRight(R right);

    @Override
    default R setValue(R value) {
        setRight(value);
        return value;
    };

    default L setKey(L key) {
        setLeft(key);
        return key;
    };

    @Override
    default L getKey() {
        return getLeft();
    }

    @Override
    default R getValue() {
        return getRight();
    }

}
