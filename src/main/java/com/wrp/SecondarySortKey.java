package com.wrp;

import scala.math.Ordered;

import java.io.Serializable;
import java.util.Objects;

/**
 * * @ClassName SecondarySortKey
 * * @Author LYleonard
 * * @Date 2019/9/17 23:58
 * * @Description 自定义二次排序的key
 * Version 1.0
 **/
public class SecondarySortKey implements Ordered<SecondarySortKey>, Serializable {

    /**
     * first: 每行的第一列
     */
    private int first;
    /**
     * second: 每行的第二列
     */
    private int second;

    public SecondarySortKey(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compare(SecondarySortKey other) {
        if (this.first - other.getFirst() != 0){
            return this.first - other.getFirst();
        } else {
            return this.second - other.getSecond();
        }
    }

    @Override
    public int compareTo(SecondarySortKey other) {
        if (this.first - other.getFirst() != 0){
            return this.first - other.getFirst();
        } else {
            return this.second - other.getSecond();
        }
    }

    @Override
    public boolean $less(SecondarySortKey other) {
        if (this.first < other.getFirst()){
            return true;
        } else if (this.first == other.getFirst() && this.second < other.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(SecondarySortKey other) {
        if (this.first > other.getFirst()){
            return true;
        }else if (this.first == other.getFirst() && this.second > other.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(SecondarySortKey other) {
        if (this.$less(other)){
            return true;
        } else if (this.first == other.getFirst() && this.second == other.getSecond()){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(SecondarySortKey other) {
        if (this.$greater(other)){
            return true;
        }else if (this.first == other.getFirst() && this.second == other.getSecond()){
            return true;
        }
        return false;
    }

    /**
     * getter，setter方法
     */
    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o){
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SecondarySortKey other = (SecondarySortKey) o;
        return first == other.first &&
                second == other.second;
    }

    @Override
    public int hashCode() {
        return Objects.hash(first, second);
    }
}
