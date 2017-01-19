package esiptestbed.mudrod.ssearch.similarity;

public class Bbox {
	public double top;
	public double bottom;
	public double right;
	public double left;
	
	public Bbox(double top, double bottom, double right, double left)
	{
		this.top = top;
		this.bottom = bottom;		
		this.right = right;
		this.left = left;
	}
	
	public double getArea()
	{
		return Math.abs((top - bottom)*(right-left));
	}

}
