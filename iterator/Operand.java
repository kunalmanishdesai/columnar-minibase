package iterator;



public class Operand {
  public  FldSpec  symbol;
  public  String   string;
  public  int      integer;
  public  float    real;

  public Operand(){}

  public Operand(Operand operand) {
    this.symbol = operand.symbol;
    this.string = operand.string;
    this.integer = operand.integer;
    this.real = operand.real;
  }
}
