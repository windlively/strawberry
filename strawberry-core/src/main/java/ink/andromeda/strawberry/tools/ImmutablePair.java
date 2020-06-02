package ink.andromeda.strawberry.tools;

import java.util.Objects;

import static ink.andromeda.strawberry.tools.GeneralTools.toJSONString;

public class ImmutablePair<L, R> implements Pair<L, R>{

    private final L left;

    private final R right;

    protected ImmutablePair(L left, R right){
        Objects.requireNonNull(left);
        Objects.requireNonNull(right);

        this.left = left;
        this.right = right;
    }

    public L getLeft() {
        return left;
    }

    public R getRight() {
        return right;
    }

    @Override
    public void setLeft(L left) {
        throw new UnsupportedOperationException("could not set value for ImmutablePair");
    }

    @Override
    public void setRight(R right) {
        throw new UnsupportedOperationException("could not set value for ImmutablePair");
    }

    @Override
    public L getKey(){
        return left;
    }

    @Override
    public R getValue() {
        return right;
    }

    @Override
    public String toString() {
        return toJSONString(this);
    }
}
