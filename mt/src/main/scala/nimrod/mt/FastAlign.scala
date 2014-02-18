package nimrod.mt

import java.io.PrintWriter
import java.util.concurrent.ConcurrentSkipListMap
import scala.collection.mutable.ListBuffer
import scala.math._
import scala.collection.JavaConversions._
import nimrod.util.Dictionary
import nimrod.services.Services
import it.unimi.dsi.fastutil.ints._


/**
 * Translation of fastalign.cc https://github.com/clab/fast_align under Apache License v2.0.
 */
object FastAlign {

  object Md {
    def digamma(x : Double) : Double = {
      var x2 = 0.0
      var result = 0.0
      while(x2 < 7) {
        result -= 1/x2
        x2 += 1
      }
      x2 -= 1.0/2.0;
      val xx = 1.0/x2;
      val xx2 = xx*xx;
      val xx4 = xx2*xx2;
      result + log(x2)+(1.0/24.0)*xx2-(7.0/960.0)*xx4+(31.0/8064.0)*xx4*xx2-(127.0/30720.0)*xx4*xx4;
    }
  }

  object DiagonalAlignment {
    def computeZ(i : Int, m : Int, n : Int, alpha : Double) : Double = {
      require(i > 0)
      require(n > 0)
      require(m >= i)
      val split = i.toDouble * n / m
      val floor = split.toInt
      val ceil = floor + 1
      val ratio = exp(-alpha / n)
      val num_top = n - floor
      val ezt = if(num_top != 0) {
        unnormalizedProb(i, ceil, m, n, alpha) * (1.0 - pow(ratio, num_top)) / (1.0 - ratio)
      } else {
        0.0
      }
      val ezb = if(floor != 0) {
        unnormalizedProb(i, floor, m, n, alpha) * (1.0 - pow(ratio, floor)) / (1.0 - ratio)
      } else {
        0.0
      }
      return ezb + ezt;
    }

    def computeDLogZ(i : Int, m : Int, n : Int, alpha : Double) : Double = {
      val z = computeZ(i, n, m, alpha);
      val split = i.toDouble * n / m;
      val floor = split.toInt
      val ceil = floor + 1;
      val ratio = exp(-alpha / n);
      val d = -1.0 / n;
      val num_top = n - floor;
      val pct =  if (num_top != 0) {
        arithmetico_geometric_series(feature(i, ceil, m, n), unnormalizedProb(i, ceil, m, n, alpha), ratio, d, num_top);
      } else {
        0.0
      }
      val pcb = if(floor != 0) {
        arithmetico_geometric_series(feature(i, floor, m, n), unnormalizedProb(i, floor, m, n, alpha), ratio, d, floor);
      } else {
        0.0
      }
      return (pct + pcb) / z;
    }

    private def arithmetico_geometric_series(a_1 : Double, g_1 : Double, r : Double, d : Double, n : Int) : Double = {
      val g_np1 = g_1 * pow(r, n);
      val a_n = d * (n - 1) + a_1;
      val x_1 = a_1 * g_1;
      val g_2 = g_1 * r;
      val rm1 = r - 1;
      return (a_n * g_np1 - x_1) / rm1 - d*(g_np1 - g_2) / (rm1 * rm1);
    }

    def unnormalizedProb(i : Int, j : Int, m : Int, n : Int, alpha : Double) : Double = exp(feature(i, j, m, n) * alpha)

    def feature(i : Int, j : Int, m : Int, n : Int) : Double = - abs(j.toDouble / n - i.toDouble / m)
  }

  class TTable {
    type Word2Double = ConcurrentSkipListMap[Int, Double]
    type Word2Word2Double = ListBuffer[Word2Double]
    var ttable : Word2Word2Double = ListBuffer[Word2Double]()
    var counts : Word2Word2Double = ListBuffer[Word2Double]()

    private def getOrElse(map : Word2Double, key : Int) : Double = {
      if(map.containsKey(key)) { 
        map.get(key)
      } else {
        0.0
      }
    }

    def prob(e : Int, f : Int) : Double = {
      if (e < ttable.size) {
        val cpd = ttable(e);
        if(cpd.containsKey(f)) {
          cpd.get(f)
        } else {
          1e-9
        }
      } else {
        1e-9
      }
    }

    def increment(e : Int, f : Int, x : Double = 1.0) {
      while(e >= counts.size) {
        counts += new Word2Double()
      }
      val c = counts(e)
      c.put(f, getOrElse(c, f) + x)
    }

    private def setProb(i : Int, j : Int, p : Double) {
      while(ttable.size <= i) {
        ttable += new Word2Double()
      }
      ttable(i).put(j, p)
    }

    def normalizeVB(alpha : Double) {
      ttable.clear()
      for(i <- 0 until counts.size) {
        val cpd = counts(i)
        var tot = 0.0
        for((word, prob) <- cpd) {
          tot += prob + alpha
        }
        if (tot == 0.0) {
           tot = 1.0
        }
        for((word, prob) <- cpd) {
          setProb(i, word, math.exp(Md.digamma(prob + alpha) - Md.digamma(tot)))
        }
      }
      counts.clear()
    }

    def normalize() {
      ttable.clear()
      for(i <- 0 until counts.size) {
        val cpd = counts(i)
        var tot = 0.0
        for((word, prob) <- cpd) {
          tot += prob 
        }
        if (tot == 0.0) {
           tot = 1.0
        }
        for((word, prob) <- cpd) {
          setProb(i, word, prob / tot)
        }
      }
      counts.clear()
    }

    def +=(rhs : TTable) {
      if(rhs.counts.size > counts.size) {
        counts ++= ListBuffer.fill(rhs.counts.size - counts.size)(new Word2Double())
      }
      for(i <- 0 until rhs.counts.size) {
        for((word, prob) <- rhs.counts(i)) {
          counts(i).put(word, getOrElse(counts(i), word) + prob)
        }
      }
    }

    def exportToFile(filename : String, d : Dictionary[String]) {
      val out = new PrintWriter(filename)
      for(i <- 0 until ttable.size) {
        val a = i
        for((word, prob) <- ttable(i)) {
          out.println("%d\t%d\t%f" format (a,word,prob))
        }
      }
      out.close()
    }

    override def toString = {
      val sb = new StringBuilder()
      var i = 0
      for(ttrow <- ttable) {
        sb.append("%d: " format (i))
        i += 1
        val it = ttrow.entrySet().iterator
        while(it.hasNext) {
          val entry = it.next
          val key = entry.getKey
          val value = entry.getValue
          sb.append("%d -> %.4f" format (key, value))
          if(it.hasNext) {
            sb.append(", ")
          }
        }
        sb.append("\n")
      }
      sb.toString
    }
  }

  class AlignResult {
    var aligns = ListBuffer[List[(Int, Int)]]()
    var likelihood = 0.0
    var base2_likelihood = 0.0
    var cross_entropy = 0.0
    var perplexity = 0.0
    var posterior_p0 = 0.0
    var posterior_al_feat = 0.0
    var size_counts = 0
    var error : Option[String] = None
    def printAlign = println(aligns.map(align_line => align_line.map(a => "%d-%d" format (a._1, a._2)).mkString(" ")).mkString("\n"))
  }

  private val d = new Dictionary[String]()
  // C++ emulation FTW :)
  private sealed trait Endl
  private object endl extends Endl
  private sealed trait Flush
  private object flush extends Flush
  private object cerr {
    def <<(s : String) = { System.err.print(s) ; this }
    def <<(i : Int) = { System.err.print(i) ; this }
    def <<(d : Double) = { System.err.print(d) ; this }
    def <<(nl : Endl) = { System.err.println() ; this }
    def <<(f : Flush) = { System.err.flush() ; this }
  }
  private object cout {
    def <<(s : String) = { System.out.print(s) ; this }
    def <<(s : Char) = { System.out.print(s) ; this }
    def <<(i : Int) = { System.out.print(i) ; this }
    def <<(d : Double) = { System.out.print(d) ; this }
    def <<(nl : Endl) = { System.out.println() ; this }
    def <<(f : Flush) = { System.out.flush() ; this }
  }


  private val tokenizer = Services.get(classOf[Tokenizer])

  def parseline(line : String, is_reverse : Boolean) : (Seq[Int], Seq[Int]) = line split " \\|\\|\\| " match {
    case Array(r,l) if is_reverse => (tokenizer.tokenize(l).map(d.lookup(_)), tokenizer.tokenize(r).map(d.lookup(_)))
    case Array(l,r) => (tokenizer.tokenize(l).map(d.lookup(_)), tokenizer.tokenize(r).map(d.lookup(_)))
    case _ => throw new RuntimeException("Bad line: " + line)
  }

  case class ProcessLineResult(
    delta_tot_len_ratio : Double,
    delta_denom : Int,
    delta_size_counts : (Int, Int),
    delta_toks : Int,
    delta_likelihood : Double,
    delta_aligns : List[(Int, Int)],
    delta_c0 : Double,
    delta_emp_feat : Double,
    error : Option[String]
  )

  def process_line(line : String, align_result : AlignResult, is_reverse : Boolean, 
    use_null : Boolean, favor_diagonal : Boolean,
    prob_align_null : Double, prob_align_not_null : Double, 
    s2t : TTable, lc : Int, 
    final_iteration : Boolean, kNULL : Int, diagonal_tension : Double) : ProcessLineResult = {
    var result_align = List[(Int,Int)]()
    val (src, trg) = parseline(line, is_reverse)
    if (src.size == 0 || trg.size == 0) {
      return ProcessLineResult(
        0.0, 0, (0,0), 0, 0.0, Nil, 0.0, 0.0, Some("Error in line %d\n%s\n" format (lc, line))
      )
    }
    val probs = ListBuffer.fill(src.size + 1)(0.0)
    var first_al = true;  // used when printing alignments
    var dl = 0.0
    var dc0 = 0.0
    var d_ef = 0.0
    for(j <- 0 until trg.size) {
      //const unsigned& f_j = trg[j];
      val f_j = trg(j)
      var sum = 0.0;
      var prob_a_i = 1.0 / (src.size + (if(use_null) { 1 } else { 0 }));  // uniform (model 1)
      if (use_null) {
        if (favor_diagonal) {
          prob_a_i = prob_align_null
        }
        probs(0) = s2t.prob(kNULL, f_j) * prob_a_i;
        sum += probs(0);
      }
      var az = 0.0
      if (favor_diagonal) {
        az = DiagonalAlignment.computeZ(j+1, trg.size, src.size, diagonal_tension) / prob_align_not_null
      }
      for(i <- 1 to src.size) {
        if (favor_diagonal) {
          prob_a_i = DiagonalAlignment.unnormalizedProb(j + 1, i, trg.size, src.size, diagonal_tension) / az
        }
        probs(i) = s2t.prob(src(i-1), f_j) * prob_a_i;
        sum += probs(i);
      }
      if (final_iteration) {
        var max_p = -1.0;
        var max_index = -1;
        if (use_null) {
          max_index = 0;
          max_p = probs(0);
        }
        for(i <- 1 to src.size) {
          if (probs(i) > max_p) {
            max_index = i;
            max_p = probs(i);
          }
        }
        if (max_index > 0) {
          if(is_reverse) {
            result_align ::= (j, max_index - 1)
          } else {
            result_align ::= (max_index - 1, j)
          }
        }
      } else {
        if (use_null) {
          val count = probs(0) / sum
          dc0 += count
          s2t.increment(kNULL, f_j, count)
        }
        for(i <- 1 to src.size) {
          val p = probs(i) / sum;
          s2t.increment(src(i-1), f_j, p);
          d_ef += DiagonalAlignment.feature(j, i, trg.size, src.size) * p;
        }
      }
      dl += log(sum)
    }

    return ProcessLineResult(
      trg.size.toDouble / src.size,
      trg.size,
      (trg.size, src.size),
      trg.size,
      dl,
      result_align,
      dc0,
      d_ef,
      None
    )
  }

  def fast_align(input : String, no_null_word : Boolean = false, variational_bayes : Boolean = false,
    alpha : Double = 0.01, ITERATIONS : Int = 5, conditional_probability_filename : String = "",
    _diagonal_tension : Double = 4.0, prob_align_null : Double = 0.08,
    favor_diagonal : Boolean = false, is_reverse : Boolean = false,
    optimize_tension : Boolean = false) : AlignResult = {
    val align_result = new AlignResult()
    var diagonal_tension : Double = _diagonal_tension // Modified at end of iteration only
    val use_null : Boolean = !no_null_word;
    if (variational_bayes && alpha <= 0.0) {
      align_result.error = Some("--alpha must be > 0\n")
      return align_result
    }
    val prob_align_not_null : Double = 1.0 - prob_align_null;
    //const unsigned kNULL = d.Convert("<eps>");
    val kNULL : Int = d.lookup("<eps>");
    var s2t = new TTable()
    val size_counts = collection.mutable.Map[(Int, Int),Int]()
    var tot_len_ratio : Double = 0.0;
    var mean_srclen_multiplier : Double = 0.0;
    for(iter <- 0 until ITERATIONS) {
      val final_iteration : Boolean = (iter == (ITERATIONS - 1));
      //cerr << "ITERATION " << (iter + 1) << (if(final_iteration) {" (FINAL)" } else { "" }) << endl;
      //ifstream in(input.c_str());
      val in = scala.io.Source.fromFile(input).getLines
      var likelihood : Double = 0.0;
      var denom : Double = 0.0;
      var lc : Int = 0; // Line count
      var flag : Boolean = false;
      var ssrc : String = ""
      var strg : String = ""
      var c0 : Double = 0.0;
      var emp_feat : Double = 0.0;
      var toks : Double = 0.0;
      for(line <- in) {
        lc += 1
        if (lc % 1000 == 0) { cerr << '.'; flag = true; }
        if (lc %50000 == 0) { cerr << " [" << lc << "]\n" << flush; flag = false; }
        val ProcessLineResult(dtlr,dd,dsc,dt,dl,da,dc0,d_ef,e) = process_line(line, align_result, is_reverse,
          use_null, favor_diagonal,
          prob_align_null, prob_align_not_null,
          s2t, lc,
          final_iteration, kNULL, diagonal_tension)
        e match {
          case Some(e) => {
            align_result.error = Some(e)
            return align_result
          }
          case None => {}
        }
        if (iter == 0) {
          tot_len_ratio += dtlr
        } 
        denom += dd
        if(iter == 0) {
          if(size_counts.contains(dsc)) {
            size_counts(dsc) += 1
          } else {
            size_counts(dsc) = 1
          }
        }
        toks += dt
        likelihood += dl
        if(final_iteration) {
          align_result.aligns += da
        }
        c0 += dc0
        emp_feat += d_ef
      } // END Read Line

      // log(e) = 1.0
      val base2_likelihood = likelihood / log(2);

      //if (flag) { cerr << endl; }
      if (iter == 0) {
        mean_srclen_multiplier = tot_len_ratio / lc;
        //cerr << "expected target length = source length * " << mean_srclen_multiplier << endl;
      }
      emp_feat /= toks;
      //cerr << "  log_e likelihood: " << likelihood << endl;
      align_result.likelihood = likelihood
      //cerr << "  log_2 likelihood: " << base2_likelihood << endl;
      align_result.base2_likelihood = base2_likelihood
      //cerr << "     cross entropy: " << (-base2_likelihood / denom) << endl;
      align_result.cross_entropy = (-base2_likelihood / denom)
      //cerr << "        perplexity: " << pow(2.0, -base2_likelihood / denom) << endl;
      align_result.perplexity = pow(2.0, -base2_likelihood / denom)
      //cerr << "      posterior p0: " << c0 / toks << endl;
      align_result.posterior_p0 = c0 / toks
      //cerr << " posterior al-feat: " << emp_feat << endl;
      align_result.posterior_al_feat = emp_feat
      //cerr << "     model tension: " << mod_feat / toks << endl;
      align_result.size_counts = size_counts.size
      //cerr << "       size counts: " << size_counts.size << endl;
      if (!final_iteration) {
        if (favor_diagonal && optimize_tension && iter > 0) {
          for(ii <- 0 until 8) {
            var mod_feat = 0.0;
            val it = size_counts.iterator
            while(it.hasNext) {
              val entry = it.next
              val p = entry._1
              for(j <- 1 to p._1) {
                mod_feat += entry._2 * DiagonalAlignment.computeDLogZ(j, p._1, p._2, diagonal_tension);
              }
            }
            mod_feat /= toks;
            //cerr << "  " << ii + 1 << "  model al-feat: " << mod_feat << " (tension=" << diagonal_tension << ")\n";
            diagonal_tension += (emp_feat - mod_feat) * 20.0;
            if (diagonal_tension <= 0.1) { diagonal_tension = 0.1 }
            if (diagonal_tension > 14) { diagonal_tension = 14 }
          }
          //cerr << "     final tension: " << diagonal_tension << endl;
        }
        if (variational_bayes) {
          s2t.normalizeVB(alpha);
        } else {
          s2t.normalize()
        }
        println(s2t.toString)
        //prob_align_null *= 0.8; // XXX
        //prob_align_null += (c0 / toks) * 0.2;
        // This seems unneccesary !
//        prob_align_not_null = 1.0 - prob_align_null;
      }
    } // END iteration
    if (conditional_probability_filename != "") {
      //cerr << "conditional probabilities: " << conditional_probability_filename << endl;
      s2t.exportToFile(conditional_probability_filename, d);
    }
    return align_result
  }
}
