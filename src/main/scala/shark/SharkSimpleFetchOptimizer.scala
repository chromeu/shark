package shark

import org.apache.hadoop.hive.ql.optimizer.SimpleFetchOptimizer
import org.apache.hadoop.hive.ql.parse.ParseContext
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.exec.Operator
import org.apache.hadoop.hive.ql.exec.TableScanOperator
import org.apache.hadoop.hive.ql.metadata.Table
import org.apache.hadoop.hive.ql.parse.SplitSample
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList
import java.util.HashSet
import java.util.HashMap
import java.util.Iterator
import org.apache.hadoop.hive.ql.hooks.ReadEntity
import org.apache.hadoop.hive.ql.exec.Operator
import org.apache.hadoop.hive.ql.metadata.Partition
import org.apache.hadoop.hive.ql.plan.OperatorDesc
import org.apache.hadoop.hive.ql.parse.QB
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner
import org.apache.commons.logging.LogFactory

class SharkSimpleFetchOptimizer extends SimpleFetchOptimizer with LogHelper{
  private val LOG = LogFactory.getLog("SharkSimpleFetchOptimizer")
  override def transform(pctx: ParseContext):ParseContext = {
      val topOps: HashMap[String, Operator[_ <: OperatorDesc]] = pctx.getTopOps()
      LOG.debug("[SharkSimpleFetchOptimizer --- transform]--topOps.size():"+topOps.size())
      val alias = pctx.getTopOps().keySet().toArray()(0).asInstanceOf[String]
      val mode = HiveConf.getVar(pctx.getConf(), HiveConf.ConfVars.HIVEFETCHTASKCONVERSION)
      val aggressive = "more".equals(mode)
      val fetchData: FetchData = pctx.getTopOps().values().toArray()(0) match {
        case topOP: TableScanOperator => {
        		  checkTree(aggressive,pctx,alias,topOP)
           }
        case _ => {
        		  null
           }
        }
      if(fetchData != null){
        fetchData.convertToWork(pctx)
        }
      return pctx;
   }
  
  private def checkTree(aggressive: Boolean, pctx: ParseContext, alias: String, ts: TableScanOperator): FetchData = {
     val splitSample: SplitSample = pctx.getNameToSplitSample().get(alias);
    if (!aggressive && splitSample != null) {
      return null;
     }
    val qb: QB = pctx.getQB();
    if (!aggressive && qb.hasTableSample(alias)) {
      return null;
     }

    val table: Table= qb.getMetaData().getAliasToTable().get(alias);
    if (table == null) {
      return null;
    }
    if (!table.isPartitioned()) {
      return new FetchData(table, splitSample)
    }

    var bypassFilter: Boolean = false;
    if (HiveConf.getBoolVar(pctx.getConf(), HiveConf.ConfVars.HIVEOPTPPD)) {
      val pruner: ExprNodeDesc = pctx.getOpToPartPruner().get(ts);
      bypassFilter = PartitionPruner.onlyContainsPartnCols(table, pruner);
    }
    if (aggressive || bypassFilter) {
      val pruned: PrunedPartitionList = pctx.getPrunedPartitions(alias, ts);
      if (aggressive || pruned.getUnknownPartns().isEmpty()) {
        bypassFilter &= pruned.getUnknownPartns().isEmpty();
        return new FetchData(pruned, splitSample)
      }
    }
    return null;
  }
  
    class FetchData(val table: Table = null,
                    val splitSample: SplitSample,
                    val partsList: PrunedPartitionList = null,
                    inputs: HashSet[ReadEntity] = new HashSet[ReadEntity]())
     {
    def this(table: Table, splitSample: SplitSample) {
       this(table,splitSample,null)
     }

    def this(partsList: PrunedPartitionList, splitSample: SplitSample) {
      this(null,splitSample,partsList)
     }

    def convertToWork(pctx: ParseContext){
      inputs.clear();
      if (table != null) {
        inputs.add(new ReadEntity(table));
        LOG.debug("[FetchData---convertToWork]--- table:"+table.toString());
        pctx.getSemanticInputs().addAll(inputs);
        return
        }
      val iter: Iterator[Partition] = partsList.getNotDeniedPartns().iterator()
      while (iter.hasNext()) {
        inputs.add(new ReadEntity(iter.next()));
        }
      val sourceTable = partsList.getSourceTable();
      if(sourceTable != null){
        	LOG.debug("[FetchData---convertToWork]--- sourceTable:"+sourceTable.toString())
        }
      inputs.add(new ReadEntity(sourceTable));
      LOG.debug("[FetchData---convertToWork]--- inputs.size():"+inputs.size())
      pctx.getSemanticInputs().addAll(inputs);
     }
  }
}