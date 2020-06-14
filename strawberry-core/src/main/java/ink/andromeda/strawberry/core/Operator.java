package ink.andromeda.strawberry.core;

import ink.andromeda.strawberry.tools.GeneralTools;
import org.springframework.util.Assert;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.Date;
import java.util.Objects;

public enum Operator {

    EQ("=", "等于"){
        @Override
        public boolean isTrue(Object left, Object... right) {
            Objects.requireNonNull(left);
            Objects.requireNonNull(right);
            if(right.length != 1) {
                throw new IllegalArgumentException("operator '=' left value length is 1");
            }
            return objectComparator.compare(left, right[0]) == 0;
        }
    },
    LT("<", "小于") {
        @Override
        public boolean isTrue(Object left, Object... right) {
            Objects.requireNonNull(left);
            Objects.requireNonNull(right);
            if(right.length != 1) {
                throw new IllegalArgumentException("operator '<' left value length is 1");
            }
            return objectComparator.compare(left, right[0]) < 0;
        }
    },
    LE("<=", "小于等于") {
        @Override
        public boolean isTrue(Object left, Object... right) {
            Objects.requireNonNull(left);
            Objects.requireNonNull(right);
            if(right.length != 1) {
                throw new IllegalArgumentException("operator '<=' left value length is 1");
            }
            return objectComparator.compare(left, right[0]) <= 0;
        }
    },
    GT(">","大于"){
        @Override
        public boolean isTrue(Object left, Object... right) {
            Objects.requireNonNull(left);
            Objects.requireNonNull(right);
            if(right.length != 1) {
                throw new IllegalArgumentException("operator '>' left value length is 1");
            }
            return objectComparator.compare(left, right[0]) > 0;
        }
    },
    GE(">=", "大于等于") {
        @Override
        public boolean isTrue(Object left, Object... right) {
            Objects.requireNonNull(left);
            Objects.requireNonNull(right);
            if(right.length != 1) {
                throw new IllegalArgumentException("operator '>=' left value length is 1");
            }
            return objectComparator.compare(left, right[0]) >= 0;
        }
    },
    NE("!=", "不等于"){
        @Override
        public boolean isTrue(Object left, Object... right) {
            Objects.requireNonNull(left);
            Objects.requireNonNull(right);
            if(right.length != 1) {
                throw new IllegalArgumentException("operator '!=' left value length is 1");
            }
            return objectComparator.compare(left, right[0]) != 0;
        }
    };

    private final String code;

    Operator(String code, String description) {
        this.code = code;
    }

    public String code(){
        return code;
    }

    public abstract boolean isTrue(Object left, Object... right);

    private static void requiredNonNull(Object... objects){
        Assert.notNull(objects);
    }

    public static Operator of(String code){
        for (Operator op : Operator.values()){
            if(op.code.equals(code))
                return op;
        }
        throw new IllegalArgumentException("unknown operator code: " + code);
    }

    public static final Comparator<Object> objectComparator = (o1, o2) -> {

        if(o1 instanceof Number){
            BigDecimal leftVal = new BigDecimal(o1.toString());
            BigDecimal rightVal;
            try {
                rightVal = GeneralTools.conversionService().convert(o2, BigDecimal.class);
                return leftVal.compareTo(rightVal);
            }catch (Exception ex){
                throw new IllegalArgumentException("left value '" + o1 + "' is number but right '" + o2 +"' could not convert to a number: " + ex.getMessage(), ex);
            }
        }

        if(o1 instanceof String) {
            if(!(o2 instanceof String)){
                throw new IllegalArgumentException("left value '" + o1 + "' is string but right '" + o2 +"' not");
            }
            return ((String) o1).compareTo((String) o2);
        }

        if(o1 instanceof Date){
            Date rightDate;
            try {
                rightDate = GeneralTools.conversionService().convert(o2, Date.class);
            }catch (Exception ex){
                throw new IllegalArgumentException("left value '" + o1 + "' is date type but right '" + o2 +"' could not convert to a date: " + ex.getMessage(), ex);
            }
            return ((Date) o1).compareTo(rightDate);
        }

        throw new IllegalArgumentException("not support compare type: " + o1.getClass().getName() + ", " + o1);
    };
}
