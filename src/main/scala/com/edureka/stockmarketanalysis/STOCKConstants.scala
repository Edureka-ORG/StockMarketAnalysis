package com.edureka.stockmarketanalysis

object STOCKConstants {

  final val MSFT_STEEP_CHANGE: String = "select dt,openPrice, " + 
                                        " closePrice, abs(closePrice - openPrice) as MSFTDiff "+
                                        "from stocksMSFT where abs(closePrice-openPrice) > 2 ";

  final val STOCKS_COMPARE_CLOSING_PRICE:String = "select stocksAAON.dt as dt, "+
                                            " stocksAAON.adjClosePrice as AAONClose, "+
                                            " stocksABAX.adjClosePrice as ABAXClose, "+
                                            " stocksFAST.adjClosePrice as FASTClose "+
                                            " FROM stocksAAON JOIN stocksABAX "+
                                            " ON stocksAAON.dt=stocksABAX.dt " +
                                            " JOIN stocksFAST "+
                                            " ON stocksAAON.dt=stocksFAST.dt order by dt desc";
  
  final val AVG_CLOSING_PER_YEAR:String = " select year(dt) as year, " +
                                          " avg(AAONClose) as AAON," +
                                          " avg(ABAXClose) as ABAX," +
                                          " avg(FASTClose) as FAST " +
                                          " FROM JoinClose "+
                                          " group by year(dt) order by year(dt)";
  
  final val COMPANY_ALL_VALUES = " select year, AAON Value, 'AAON' Company from newTable UNION ALL" +
                                 " select year, ABAX Value, 'ABAX' Company from newTable UNION ALL" +
                                 " select year, FAST Value, 'FAST' Company from newTable";
  
  final val BEST_AVG_CLOSING =  " select year, GREATEST((AAON),(ABAX),(FAST)) as BestCompany from newTable ";
  
  final val FINAL_TABLE = " Select CompanyAll.year, BestCompanyYear.BestCompany,CompanyAll.Company " +
                          " FROM BestCompanyYear LEFT JOIN CompanyAll "+
                          " ON CompanyAll.value = BestCompanyYear.BestCompany " +
                          " ORDER BY CompanyAll.year ";
}