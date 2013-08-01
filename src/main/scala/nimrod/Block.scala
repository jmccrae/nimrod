package nimrod

class Block(name : String, wf : Workflow) extends Task {
  private var tasks : List[Task] = Nil

  def add(task : Task) = {
    tasks ::= task
  }

  def update(old : Task, task : Task) = {
    tasks = tasks map (t => {
        if(t eq old) {
          task
        } else {
          t
        }
      })
  }

  override def exec : Int = {
    var x = 1
    val totalSubsteps = tasks.size
    for(task <- tasks.reverse) {      
      for(req <- task.requirements) {
        if(!req.validInput) {
          throw new WorkflowException("Requirement not satisified " + req)
        }
      }
      println("[\033[0;32m " + wf.currentStep + "." + x + " / " + wf.totalSteps + "." + totalSubsteps + " \033[m] Start: " + task)
      val result = task.exec
      if(result != 0) {
        println("[\033[0;31m " + wf.currentStep + "." + x + " / " + wf.totalSteps + "." + totalSubsteps + " \033[m] Failed: " + task)
        return result
      }
      println("[\033[0;32m " + wf.currentStep + "." + x + " / " + wf.totalSteps + "." + totalSubsteps + " \033[m] Finished: " + task)
      x += 1
    }    
    return 0
  }

  override def toString = name + " block"
  def printTasks { println(tasks.mkString("\n")) }
}

object block {
  def apply(name : String)(desc : => Unit)(implicit workflow : Workflow) = {
    val b = new Block(name,workflow)
    workflow.startBlock(b)
    desc
    workflow.endBlock(b)
    b
  }
}
