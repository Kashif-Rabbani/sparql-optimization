package dk.uni.cs.utils;

import java.util.Objects;

public class Tuple2<X, Y> {
    public final X _1;
    public final Y _2;

    public Tuple2(X _1, Y _2) {
        this._1 = _1;
        this._2 = _2;
    }
    public X _1() {
        return _1;
    }
    public Y _2() {
        return _2;
    }
    @Override
    public String toString() {
        return "dk.uni.cs.utils.Tuple3{" +
                "_1=" + _1 +
                ", _2=" + _2 +
                '}';
    }
    public int hashCode() {
        return Objects.hash(_1,_2);
    }
    @Override
    public boolean equals(Object o) {
        if (o instanceof Tuple3) {
            final Tuple3 other = (Tuple3)o;
            return Objects.equals(_1,other._1) && Objects.equals(_2,other._2);
        }
        else {
            return false;
        }
    }
}