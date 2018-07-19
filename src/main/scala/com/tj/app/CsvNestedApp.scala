package com.tj.app

import com.tj.common.BaseDriver

object NestedRdd extends BaseDriver {

  // val res = mappedRdd.groupByKey().map{ case(x, y) => (x, y.toList) }.collect

  // https://stackoverflow.com/questions/45131481/aggregation-function-collect-list-or-collect-set-over-window
  // https://stackoverflow.com/questions/37737843/aggregating-multiple-columns-with-custom-function-in-spark

}


