package dk.uni.cs.utils;

import java.util.Objects;

public class Tuple5<X, Y, Z, K, L> {
    public final X _1;
    public final Y _2;
    public final Z _3;
    public final K _4;
    public final L _5;
    public Tuple5(X _1, Y _2, Z _3, K _4, L _5) {
        this._1 = _1;
        this._2 = _2;
        this._3 = _3;
        this._4 = _4;
        this._5 = _5;
    }
    public X _1() {
        return _1;
    }
    public Y _2() {
        return _2;
    }
    public Z _3() { return _3; }
    public K _4() { return _4; }
    public L _5() { return _5; }

    @Override
    public String toString() {
        return "dk.uni.cs.utils.Tuple3{" +
                "_1=" + _1 +
                ", _2=" + _2 +
                ", _3=" + _3 +
                ", _4=" + _4 +
                ", _5=" + _5 +
                '}';
    }
    public int hashCode() {
        return Objects.hash(_1,_2,_3, _4, _5);
    }
    @Override
    public boolean equals(Object o) {
        if (o instanceof Tuple5) {
            final Tuple5 other = (Tuple5)o;
            return Objects.equals(_1,other._1)
                    && Objects.equals(_2,other._2)
                    && Objects.equals(_3,other._3)
                    && Objects.equals(_4,other._4)
                    && Objects.equals(_5,other._5);

        }
        else {
            return false;
        }
    }
}