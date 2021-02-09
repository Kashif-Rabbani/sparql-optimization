package dk.uni.cs.extras;

import java.util.*;

abstract class Tree<E> {

    protected double card;
    protected double cost;

    //public abstract String toString();
    public abstract Set<E> getElements();
    public abstract E getOneElement();
    public double getCard() {
        return this.card;
    }
    public void setCard(double c) {
        this.card = c;
    }
    public double getCost() {
        return this.cost;
    }
    public void setCost(double c) {
        this.cost = c;
    }
    public String toString() {
        return "("+card+", "+cost+")";
    }
}

class Branch<E> extends Tree<E> {

    private Tree<E> left;
    private Tree<E> right;

    public Branch(Tree<E> l, Tree<E> r) {
        this.left = l;
        this.right = r;
    }

    public void setLeft(Tree<E> l) {
        this.left = l;
    }

    public void setRight(Tree<E> r) {
        this.right= r;
    }

    public Tree<E> getLeft() {
        return this.left;
    }

    public Tree<E> getRight() {
        return this.right;
    }

    public String toString() {
        return super.toString()+"{" + this.left.toString() + " . " + this.right.toString() +"}";
    }

    public Set<E> getElements() {

        HashSet<E> set = new HashSet<E>();
        set.addAll(left.getElements());
        set.addAll(right.getElements());
        return set;
    }

    public E getOneElement() {
        return left.getOneElement();
    }
}

class Leaf<E> extends Tree<E> {

    private E elem;

    public Leaf(E e) {
        this.elem = e;
    }

    public String toString() {
        return super.toString()+elem.toString();
    }

    public Set<E> getElements() {

        HashSet<E> set = new HashSet<E>();
        set.add(elem);
        return set;
    }

    public E getOneElement() {
        return elem;
    } 
}
