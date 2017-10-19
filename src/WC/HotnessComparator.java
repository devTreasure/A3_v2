package WC;

import java.util.Comparator;

public class HotnessComparator implements Comparator<Hotness> {

	@Override
	public int compare(Hotness a1, Hotness a2) {
		return Float.compare(a2.hotNum, a1.hotNum);
	}
	
}
