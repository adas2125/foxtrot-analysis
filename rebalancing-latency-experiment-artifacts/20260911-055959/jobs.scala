val records = 1000000
val runID = "20260911-055959"
val base = CoreJob(
  dbname = "de.unihamburg.informatik.nosqlmark.db.RedisClusterClient",
  dbproperties = Map(
    "redis.seeds" -> "redis-20260911-055959-0.redis-20260911-055959-headless.foxtrot-rebalance-20260911-055959.svc.cluster.local:6379,redis-20260911-055959-1.redis-20260911-055959-headless.foxtrot-rebalance-20260911-055959.svc.cluster.local:6379",
    "redis.pool.max" -> "100"
  ),
  workload = "CoreWorkload", table = "usertable", phase = "transactional",
  target = 500.0, nodes = 1, worker = 1, asyncmode = true,
  counts = CoreCounts(recordcount = records, warmupcount = 0,
    operationcount = 1, insertcount = 0, insertstart = 0,
    fieldcount = 10, fieldlength = 100, readallfields = true, writeallfields = true),
  proportions = CoreProportions(readproportion = 0.8, updateproportion = 0.2,
    insertproportion = 0.0, scanproportion = 0.0, readmodifywriteproportion = 0.0),
  distributions = CoreDistributions(requestdistribution = "uniform", insertorder = "hashed"),
  loadgeneration = CoreLoadGeneration(interrequesttimedistribution = "constant"),
  logmeasurements = false, logjvmstats = false
)
val loadJob = base.copy(jobID = nc.genID, batchname = "load-" + runID,
  phase = "load", target = 2000.0,
  counts = base.counts.copy(operationcount = records, insertcount = records))
def calibration(rate: Int) = base.copy(jobID = nc.genID,
  batchname = "calibration-" + runID + "-" + rate, target = rate.toDouble,
  counts = base.counts.copy(warmupcount = 30 * rate, operationcount = 90 * rate))
def pilot = {
  val rate = new String(java.nio.file.Files.readAllBytes(
    java.nio.file.Paths.get("/NoSQLMark/artifacts/pilot-rate")), "UTF-8").trim.toInt
  base.copy(jobID = nc.genID, batchname = "foxtrot-rebalancing-" + runID,
    target = rate.toDouble, logmeasurements = true,
    counts = base.counts.copy(warmupcount = 60 * rate, operationcount = 480 * rate))
}
