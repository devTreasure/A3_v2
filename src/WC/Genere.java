package WC;

import java.util.Comparator;

public class Genere  implements Comparator{
	public String GenereName;
	public int  hotCount;
	
	public Genere()
	{
		
	}
	
	@Override	
	public int compare(Object o1, Object o2) {
		Genere g1=  (Genere) o1;
		Genere g2=  (Genere) o2;
		
		return  Integer.compare(g2.hotCount-g1.hotCount, hotCount);
		
	
	}



	public Genere(String genereName, int hotCount) {
		super();
		GenereName = genereName;
		this.hotCount = hotCount;
	}

	

}
