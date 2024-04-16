package global;

public abstract class ValueClass<T> extends Object {

    protected T value;
    public abstract T getValue();

    public abstract String toString();

    public abstract int compareTo (T key);
}