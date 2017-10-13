package WC;

import java.util.Comparator;

public class Hotness implements Comparator {
	float hotNum;
	String genere;
	String artistname;
	String songName;

	public Hotness()
	{
		
	}

	public Hotness(float hotNum, String artistname, String songName) {
		super();
		this.hotNum = hotNum;
		this.genere = genere;
		this.artistname = artistname;
		this.songName = songName;
	}


	@Override
	public int compare(Object o1, Object o2) {
		Hotness a1=(Hotness)o1;
		Hotness a2=(Hotness)o2;
		
      return Float.compare(a1.hotNum-a2.hotNum, hotNum); //
		
	}

}
