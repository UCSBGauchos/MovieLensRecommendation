import java.util.ArrayList;

public class SortedArrayList<T> extends ArrayList<T>{
	public boolean add(T value) {
		super.add(value);
		Comparable<T> cmp = (Comparable<T>) value;

		for (int i = size() - 1; i > 0 && cmp.compareTo(get(i - 1)) < 0; i--) {
			T tmp = get(i);
			set(i, get(i - 1));
			set(i - 1, tmp);
		}
		if (size() > 10)
			super.remove(10);
		return true;
	}
}
