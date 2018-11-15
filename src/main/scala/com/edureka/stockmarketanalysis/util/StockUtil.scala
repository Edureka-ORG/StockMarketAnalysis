package com.edureka.stockmarketanalysis.util

import com.edureka.stockmarketanalysis.Stock
import org.apache.commons.lang.StringUtils

object StockUtil {
  
  def parseStock(iLine:String):Stock = 
  {
    if(StringUtils.isNotEmpty(iLine))
    {
      val tokens = StringUtils.splitPreserveAllTokens(iLine, ",");
      
      new Stock(tokens(0),
          tokens(1).toDouble,
          tokens(2).toDouble,
          tokens(3).toDouble,
          tokens(4).toDouble,
          tokens(5).toDouble,
          tokens(6).toDouble
          )
      
    }else{
      return null;
    }
  }
}