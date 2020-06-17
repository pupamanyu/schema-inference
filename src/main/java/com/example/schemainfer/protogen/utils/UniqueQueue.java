package com.example.schemainfer.protogen.utils;

import java.util.*;

public class UniqueQueue<T> implements Queue<T>, Set<T> {
    private final Set<T> data = new LinkedHashSet<T>();

    public int size() {
        return data.size();
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    public boolean contains(Object o) {
        return data.contains(o);
    }

    public Iterator<T> iterator() {
        return data.iterator();
    }

    public Object[] toArray() {
        return data.toArray();
    }

    public <T> T[] toArray(T[] a) {
        return data.toArray(a);
    }

    public boolean add(T t) {
        return data.add(t);
    }

    public boolean offer(T t) {
        return add(t);
    }

    public T remove() {
        final Iterator<T> iterator = data.iterator();
        T t = iterator.next();
        iterator.remove();
        return t;
    }

    public T poll() {
        final Iterator<T> iterator = data.iterator();
        if (iterator.hasNext()) {
            T t = iterator.next();
            iterator.remove();
            return t;
        }
        return null;
    }

    public T element() {
        return data.iterator().next();
    }

    public T peek() {
        final Iterator<T> iterator = data.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    public boolean remove(Object o) {
        return data.remove(o);
    }

    public boolean containsAll(Collection<?> c) {
        return data.containsAll(c);
    }

    public boolean addAll(Collection<? extends T> c) {
        return data.addAll(c);
    }

    public boolean retainAll(Collection<?> c) {
        return data.retainAll(c);
    }

    public boolean removeAll(Collection<?> c) {
        return data.removeAll(c);
    }

    public void clear() {
        data.clear();
    }

    public boolean equals(Object o) {
        return data.equals(o);
    }

    public int hashCode() {
        return data.hashCode();
    }

    public String print() {
        StringBuffer buff = new StringBuffer() ;
        final Iterator<T> iterator = data.iterator();
        while (iterator.hasNext()) {
            buff.append(iterator.next()).append(" --> ");
        }
        return buff.toString();
    }
}