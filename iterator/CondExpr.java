package iterator;
import java.lang.*;
import java.io.*;
import global.*;

/**
 *  This clas will hold single select condition
 *  It is an element of linked list which is logically
 *  connected by OR operators.
 */

public class CondExpr {
  
  /**
   * Operator like "<"
   */
  public AttrOperator op;    
  
  /**
   * Types of operands, Null AttrType means that operand is not a
   * literal but an attribute name
   */    
  public AttrType     type1;
  public AttrType     type2;    
 
  /**
   *the left operand and right operand 
   */ 
  public Operand operand1;
  public Operand operand2;
  
  /**
   * Pointer to the next element in linked list
   */    
  public CondExpr    next;   
  
  /**
   *constructor
   */
  public  CondExpr() {
    
    operand1 = new Operand();
    operand2 = new Operand();
    
    operand1.integer = 0;
    operand2.integer = 0;
    
    next = null;
  }

  public CondExpr(CondExpr condExpr) {
    this.op = condExpr.op;
    this.type1 = condExpr.type1;
    this.type2 = condExpr.type2;
    this.operand1 = new Operand(condExpr.operand1);
    this.operand2 = new Operand(condExpr.operand2);


    if (condExpr.next != null) {
      this.next = new CondExpr(condExpr.next);
    } else {
      this.next = null;
    }
  }
}

