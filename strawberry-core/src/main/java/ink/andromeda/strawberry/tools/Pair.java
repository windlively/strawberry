package ink.andromeda.strawberry.tools;

import java.util.Map;
import java.util.Objects;

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

    @Override
    default boolean equals(Object o) {
        return this == o ||
               (o instanceof Pair && Objects.equals(((Pair<?, ?>) o).getLeft(), this.getLeft())
                && Objects.equals(((Pair<?, ?>) o).getRight(), this.getRight()));
    }

    @Override
    default int hashCode() {
        return Objects.hash(getLeft(), getRight());
    }

}
