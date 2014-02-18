package nimrod.mt

import nimrod._
import nimrod.streams.SeqStreamable
import nimrod.util.Dictionary
import nimrod.util.atoms._
import java.util.concurrent.ConcurrentSkipListMap
import scala.collection.mutable.ListBuffer
import scala.math._

package fast_align {

  class Context {

  }

  /** Store by second index! */
  class TTableRow extends Serializable {
    private val data = new ConcurrentSkipListMap[Int, Double]()

    def prob(e : Int) : Double = {
      if(data.containsKey(e)) {
        data.get(e)
      } else {
        1e-9
      }
    }

    // Not thread-safe!
    def set(i : Int, p : Double) = data.put(i, p)
    def add(i : Int, p : Double) = if(data containsKey i) {
      data.put(i, data.get(i) + p)
    } else {
      data.put(i, p)
    }

    override def toString = {
      val sb = new StringBuffer()
      sb.append("TTableRow(")
      val it = data.entrySet().iterator
      while(it.hasNext) {
        val e = it.next
        val k = e.getKey()
        val v = e.getValue()
        sb.append("%s -> %.4f" format (k,v))
        sb.append(",")
      }
      if(data.size > 0) {
        sb.deleteCharAt(sb.length - 1)
      }
      sb.append(")")
      sb.toString
    }
  }

  object TTableRow {
    def from(vals : Seq[(Int, Double)]) = {
      val elem = new TTableRow()
      for((k, v) <- vals) {
        elem.add(k, v)
      }
      elem
    }
  }

  class IterationVariables {
    var likelihood = AtomDouble()
    var c0 = AtomDouble()
    var emp_feat = AtomDouble()
    var toks = AtomInt()
  }
}

class ParallelFastAlign(val args : Seq[String]) extends Context {
  val name = "Parallel Fast Align"

  //////////////////////
  // Functions
  def parseline(line : String, is_reverse : Boolean) : (Seq[Int], Seq[Int]) = {
    line split " \\|\\|\\| " match {
      case Array(r,l) if is_reverse => 
        (tokenizer.tokenize(l).map(d.lookup(_)), tokenizer.tokenize(r).map(d.lookup(_)))
      case Array(l,r) => 
        (tokenizer.tokenize(l).map(d.lookup(_)), tokenizer.tokenize(r).map(d.lookup(_)))
      case _ => throw new RuntimeException("Bad line: " + line)
    }
  }

  /**
   * Map each line out
   */
  def preProcessLine(line : String, is_reverse : Boolean, lc : Int, vars : fast_align.IterationVariables) = {
    val (src, trg) = parseline(line, is_reverse)
    if(src.size == 0 || trg.size == 0) {
      throw new RuntimeException("Error in line %d\n%s\n" format (lc, line))
    }
    vars.toks += trg.size

    for(j <- 0 until trg.size) yield {
      //println("%d => (%d, %d, (%s))" format (trg(j), j, trg.size, src.mkString(",")))
      (trg(j), (j, trg.size, src, lc))
    }
  }

  private def _processLine(f_j : Int, data : (Int, Int, Seq[Int], Int),
    map : fast_align.TTableRow, vars : fast_align.IterationVariables) = {
      val (j, trgSize, src, lc) = data
    val probs = ListBuffer.fill(src.size + 1)(0.0)
    var sum = 0.0
    var prob_a_i = 1.0 / (src.size + (if(use_null) { 1 } else { 0 }));  // uniform (model 1)
    if (use_null) {
      if (favor_diagonal) {
        prob_a_i = prob_align_null
      }
      probs(0) = map.prob(kNULL) * prob_a_i;
      sum += probs(0);
    }
    var az = 0.0
    if (favor_diagonal) {
      az = FastAlign.DiagonalAlignment.computeZ(j+1, trgSize, src.size, diagonal_tension) / prob_align_not_null
    }
    for(i <- 1 to src.size) {
      if (favor_diagonal) {
        prob_a_i = FastAlign.DiagonalAlignment.unnormalizedProb(j + 1, i, trgSize, src.size, diagonal_tension) / az
      }
      probs(i) = map.prob(src(i-1)) * prob_a_i;
      sum += probs(i);
    }
    vars.likelihood += log(sum)
    (sum, probs)
  }


  /**
   * Calculate probabilities for each line
   */
  def processLine(f_j : Int, data : (Int, Int, Seq[Int], Int), 
    map : fast_align.TTableRow, vars : fast_align.IterationVariables) : 
      Seq[(Int, (Int, Double))] = {
    val (j, trgSize, src, lc) = data
    val (sum, probs) = _processLine(f_j, data, map, vars)
    val count_updates = for(i <- (1 to src.size).toList) yield {
      val p = probs(i) / sum;
      vars.emp_feat += FastAlign.DiagonalAlignment.feature(j, i, trgSize, src.size)
      (src(i - 1), (f_j, p))
    }

    if (use_null) {
      val count = probs(0) / sum
      vars.c0 += count
      (kNULL, (f_j, count)) :: count_updates
    } else {
      count_updates
    }
  }

  /**
   * Calculate probability and yield alignment
   */
  def processFinalLine(f_j : Int, data : (Int, Int, Seq[Int], Int),
    map : fast_align.TTableRow, vars : fast_align.IterationVariables) : Option[(Int, (Int, Int))] = {
    val (j, trgSize, src, lc) = data
    val (sum, probs) = _processLine(f_j, data, map, vars)
    val max_index = probs.zipWithIndex.maxBy(_._1)._2
    if(max_index > 0) {
      if(is_reverse) {
        Some((lc, (j, max_index - 1)))
      } else {
        Some((lc, (max_index - 1, j)))
      }
    } else {
      None
    }
  }

  /**
   * Renormalize the table
   */
  def normalize(i : Int, cpd : Seq[(Int, Double)]) = {
    var tot = 0.0
    for((word, prob) <- cpd) {
      tot += prob
    }
    if(tot == 0.0) {
      tot = 1.0
    }
    for((word, prob) <- cpd) yield {
      (word, prob / tot)
    }
  }

  /**
   * Renormalized the table
   */
  def normalizeVB(i : Int, cpd : Seq[(Int, Double)]) = {
    var tot = 0.0
    for((word, prob) <- cpd) {
      tot += prob + alpha
    }
    if(tot == 0.0) {
      tot = 1.0
    }
    for((word, prob) <- cpd) yield {
      (word, exp(FastAlign.Md.digamma(prob + alpha) - FastAlign.Md.digamma(tot)))
    }
  }

  //////////////////////
  // Workflow definition
  //
  val tokenizer = services.Services.get(classOf[Tokenizer])
  val no_null_word : Boolean = false
  val variational_bayes : Boolean = false
  val alpha : Double = 0.01
  val ITERATIONS : Int = 5
  val conditional_probability_filename : String = ""
  var diagonal_tension : Double = 4.0
  val prob_align_null : Double = 0.08
  val favor_diagonal : Boolean = false
  val is_reverse : Boolean = false
  val optimize_tension : Boolean = false
  val d = new Dictionary[String]()
  val tot_line_ratio = AtomDouble()

  val corpus = opts.roFile("corpus", "The corpus as a paired file separated by triple bars")
  val outFile = opts.woFile("output", "Where to write the alignments to")

  opts.verify

  val use_null = !no_null_word

  if(variational_bayes && alpha <= 0.0) {
    throw new WorkflowException("alpha must be > 0")
  }
  val prob_align_not_null = 1.0 - prob_align_null
  val kNULL : Int = d.lookup("<eps>");
  val size_counts = collection.mutable.Map[(Int, Int),Int]()
  var tot_len_ratio : Double = 0.0;
  var mean_srclen_multiplier : Double = 0.0;
    
  val out = outFile.asStream

  var s2t = new streams.MapStreamable[Int, fast_align.TTableRow]("s2t")

  for(iter <- 0 until ITERATIONS) {
    val final_iteration : Boolean = (iter == (ITERATIONS - 1))

    val iterationVariables = List.fill(ITERATIONS) {
      new fast_align.IterationVariables()
    }

    val preMap = (corpus.lines(monitor).map {
      (lc, line) => {
        preProcessLine(line, is_reverse, lc, iterationVariables(iter))
      }
    }).save()


    if(final_iteration) {
      val align = (preMap() cogroup s2t) map {
        case (token, (lines, Seq(map))) => lines flatMap {
          line => processFinalLine(token, line, map, iterationVariables(iter))
        }
        case (token, (lines, Seq())) => lines flatMap {
          line => processFinalLine(token, line, new fast_align.TTableRow(), iterationVariables(iter))
        }
      } forReduce {
        case (lc, aligns) => {
          out.println(aligns.sortBy(_._1).map {
            case (x,y) => "%d-%d" format (x,y)
          }.mkString(" "))
        }
      }

      task("close out") {
        out.flush
        out.close
      }
    } else {
      val postMap = (preMap() cogroup s2t) map {
        case (token, (lines, Seq(map))) => lines flatMap {
          line => processLine(token, line, map, iterationVariables(iter))
        }
        case (token, (lines, Seq())) => lines flatMap {
          line => processLine(token, line, new fast_align.TTableRow(), iterationVariables(iter))
        }
      } reduce { 
        if(variational_bayes) {
          normalizeVB
        } else {
          normalize
        }
      } mapOne {
        case (i, (j, p)) => (j, (i, p))
      } reduceOne {
        (j, vals) => {
        //  println("%d => %s" format(j, vals.mkString(",")))
          fast_align.TTableRow.from(vals)
        }
      } save()
    
      task("Clear map") {
        s2t.clear()
      }

      postMap() foreach {
        (k, row) => {
          println("%d => %s" format (k, row))
          s2t.put(k, row)      
        }
      }
    }

    task("Print output") {
      val likelihood = iterationVariables(iter).likelihood.get
      val c0 = iterationVariables(iter).c0.get
      val toks = iterationVariables(iter).toks.get
      val emp_feat = iterationVariables(iter).emp_feat.get
      val base2_likelihood = likelihood / log(2)

      println("  log_e likelihood: %.4f" format (likelihood))
      println("  log_2 likelihood: %.4f" format (base2_likelihood))
      println("     cross entropy: %.4f" format (-base2_likelihood / toks))
      println("        perplexity: %.4f" format (pow(2.0, -base2_likelihood / toks)))
      println("      posterior p0: %.4f" format (c0 / toks))
      println(" posterior al-feat: %.4f" format (emp_feat))
    }
  }
}
